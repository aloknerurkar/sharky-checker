package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/ethersphere/bee/pkg/cac"
	"github.com/ethersphere/bee/pkg/localstore"
	"github.com/ethersphere/bee/pkg/postage"
	"github.com/ethersphere/bee/pkg/sharky"
	"github.com/ethersphere/bee/pkg/shed"
	"github.com/ethersphere/bee/pkg/soc"
	"github.com/ethersphere/bee/pkg/swarm"
)

var path = flag.String("path", "./localstore", "path to localstore directory")

type dirFS struct {
	basedir string
}

func (d *dirFS) Open(path string) (fs.File, error) {
	return os.OpenFile(filepath.Join(d.basedir, path), os.O_RDWR|os.O_CREATE, 0644)
}

func main() {
	flag.Parse()

	if _, err := os.Stat(*path); os.IsNotExist(err) {
		fmt.Println("path should be full path to localstore directory")
		return
	}

	sh, err := shed.NewDB(*path, nil)
	if err != nil {
		fmt.Printf("failed initializing shed %v\n", err)
		return
	}

	defer sh.Close()

	// instantiate sharky instance
	sharkyBasePath := filepath.Join(*path, "sharky")
	if _, err := os.Stat(sharkyBasePath); os.IsNotExist(err) {
		fmt.Printf("sharky directory missing")
		return
	}

	sharkyBase := &dirFS{basedir: sharkyBasePath}

	sharkyStore, err := sharky.New(sharkyBase, 32, swarm.SocMaxChunkSize)
	if err != nil {
		fmt.Printf("failed initializing sharky %v\n")
		return
	}

	schemaName, err := sh.NewStringField("schema-name")
	if err != nil {
		fmt.Printf("failed initializing sharky %v\n")
		return
	}

	schemaNameStr, err := schemaName.Get()
	if err != nil {
		fmt.Printf("failed reading schema %v\n")
		return
	}
	if schemaNameStr != localstore.DBSchemaCurrent {
		fmt.Printf("incorrect schema, needs %s\n", localstore.DBSchemaCurrent)
		return
	}

	// Persist gc size.
	gcSize, err := sh.NewUint64Field("gc-size")
	if err != nil {
		fmt.Printf("failed reading gc-size %v\n")
		return
	}

	// reserve size
	reserveSize, err := sh.NewUint64Field("reserve-size")
	if err != nil {
		fmt.Printf("failed reading reserveSize %v\n")
		return
	}

	// Index storing actual chunk address, data and bin id.
	headerSize := 16 + postage.StampSize
	retrievalDataIndex, err := sh.NewIndex("Address->StoreTimestamp|BinID|BatchID|BatchIndex|Sig|Location", shed.IndexFuncs{
		EncodeKey: func(fields shed.Item) (key []byte, err error) {
			return fields.Address, nil
		},
		DecodeKey: func(key []byte) (e shed.Item, err error) {
			e.Address = key
			return e, nil
		},
		EncodeValue: func(fields shed.Item) (value []byte, err error) {
			b := make([]byte, headerSize)
			binary.BigEndian.PutUint64(b[:8], fields.BinID)
			binary.BigEndian.PutUint64(b[8:16], uint64(fields.StoreTimestamp))
			stamp, err := postage.NewStamp(fields.BatchID, fields.Index, fields.Timestamp, fields.Sig).MarshalBinary()
			if err != nil {
				return nil, err
			}
			copy(b[16:], stamp)
			value = append(b, fields.Location...)
			return value, nil
		},
		DecodeValue: func(keyItem shed.Item, value []byte) (e shed.Item, err error) {
			e.StoreTimestamp = int64(binary.BigEndian.Uint64(value[8:16]))
			e.BinID = binary.BigEndian.Uint64(value[:8])
			stamp := new(postage.Stamp)
			if err = stamp.UnmarshalBinary(value[16:headerSize]); err != nil {
				return e, err
			}
			e.BatchID = stamp.BatchID()
			e.Index = stamp.Index()
			e.Timestamp = stamp.Timestamp()
			e.Sig = stamp.Sig()
			e.Location = value[headerSize:]
			return e, nil
		},
	})
	if err != nil {
		fmt.Printf("failed initializing index %v\n")
		return
	}
	// Index storing access timestamp for a particular address.
	// It is needed in order to update gc index keys for iteration order.
	retrievalAccessIndex, err := sh.NewIndex("Address->AccessTimestamp", shed.IndexFuncs{
		EncodeKey: func(fields shed.Item) (key []byte, err error) {
			return fields.Address, nil
		},
		DecodeKey: func(key []byte) (e shed.Item, err error) {
			e.Address = key
			return e, nil
		},
		EncodeValue: func(fields shed.Item) (value []byte, err error) {
			b := make([]byte, 8)
			binary.BigEndian.PutUint64(b, uint64(fields.AccessTimestamp))
			return b, nil
		},
		DecodeValue: func(keyItem shed.Item, value []byte) (e shed.Item, err error) {
			e.AccessTimestamp = int64(binary.BigEndian.Uint64(value))
			return e, nil
		},
	})
	if err != nil {
		fmt.Printf("failed initializing index %v\n")
		return
	}
	// pull index allows history and live syncing per po bin
	pullIndex, err := sh.NewIndex("PO|BinID->Hash", shed.IndexFuncs{
		EncodeKey: func(fields shed.Item) (key []byte, err error) {
			key = make([]byte, 9)
			// key[0] = db.po(swarm.NewAddress(fields.Address))
			binary.BigEndian.PutUint64(key[1:9], fields.BinID)
			return key, nil
		},
		DecodeKey: func(key []byte) (e shed.Item, err error) {
			e.BinID = binary.BigEndian.Uint64(key[1:9])
			return e, nil
		},
		EncodeValue: func(fields shed.Item) (value []byte, err error) {
			value = make([]byte, 64) // 32 bytes address, 32 bytes batch id
			copy(value, fields.Address)
			copy(value[32:], fields.BatchID)
			return value, nil
		},
		DecodeValue: func(keyItem shed.Item, value []byte) (e shed.Item, err error) {
			e.Address = value[:32]
			e.BatchID = value[32:64]
			return e, nil
		},
	})
	if err != nil {
		fmt.Printf("failed initializing index %v\n")
		return
	}
	// push index contains as yet unsynced chunks
	pushIndex, err := sh.NewIndex("StoreTimestamp|Hash->Tags", shed.IndexFuncs{
		EncodeKey: func(fields shed.Item) (key []byte, err error) {
			key = make([]byte, 40)
			binary.BigEndian.PutUint64(key[:8], uint64(fields.StoreTimestamp))
			copy(key[8:], fields.Address)
			return key, nil
		},
		DecodeKey: func(key []byte) (e shed.Item, err error) {
			e.Address = key[8:]
			e.StoreTimestamp = int64(binary.BigEndian.Uint64(key[:8]))
			return e, nil
		},
		EncodeValue: func(fields shed.Item) (value []byte, err error) {
			tag := make([]byte, 4)
			binary.BigEndian.PutUint32(tag, fields.Tag)
			return tag, nil
		},
		DecodeValue: func(keyItem shed.Item, value []byte) (e shed.Item, err error) {
			if len(value) == 4 { // only values with tag should be decoded
				e.Tag = binary.BigEndian.Uint32(value)
			}
			return e, nil
		},
	})
	if err != nil {
		fmt.Printf("failed initializing index %v\n")
		return
	}
	// gc index for removable chunk ordered by ascending last access time
	gcIndex, err := sh.NewIndex("AccessTimestamp|BinID|Hash->BatchID|BatchIndex", shed.IndexFuncs{
		EncodeKey: func(fields shed.Item) (key []byte, err error) {
			b := make([]byte, 16, 16+len(fields.Address))
			binary.BigEndian.PutUint64(b[:8], uint64(fields.AccessTimestamp))
			binary.BigEndian.PutUint64(b[8:16], fields.BinID)
			key = append(b, fields.Address...)
			return key, nil
		},
		DecodeKey: func(key []byte) (e shed.Item, err error) {
			e.AccessTimestamp = int64(binary.BigEndian.Uint64(key[:8]))
			e.BinID = binary.BigEndian.Uint64(key[8:16])
			e.Address = key[16:]
			return e, nil
		},
		EncodeValue: func(fields shed.Item) (value []byte, err error) {
			value = make([]byte, 40)
			copy(value, fields.BatchID)
			copy(value[32:], fields.Index)
			return value, nil
		},
		DecodeValue: func(keyItem shed.Item, value []byte) (e shed.Item, err error) {
			e.BatchID = make([]byte, 32)
			copy(e.BatchID, value[:32])
			e.Index = make([]byte, postage.IndexSize)
			copy(e.Index, value[32:])
			return e, nil
		},
	})
	if err != nil {
		fmt.Printf("failed initializing index %v\n")
		return
	}

	// Create a index structure for storing pinned chunks and their pin counts
	pinIndex, err := sh.NewIndex("Hash->PinCounter", shed.IndexFuncs{
		EncodeKey: func(fields shed.Item) (key []byte, err error) {
			return fields.Address, nil
		},
		DecodeKey: func(key []byte) (e shed.Item, err error) {
			e.Address = key
			return e, nil
		},
		EncodeValue: func(fields shed.Item) (value []byte, err error) {
			b := make([]byte, 8)
			binary.BigEndian.PutUint64(b[:8], fields.PinCounter)
			return b, nil
		},
		DecodeValue: func(keyItem shed.Item, value []byte) (e shed.Item, err error) {
			e.PinCounter = binary.BigEndian.Uint64(value[:8])
			return e, nil
		},
	})
	if err != nil {
		fmt.Printf("failed initializing index %v\n")
		return
	}

	postageChunksIndex, err := sh.NewIndex("BatchID|PO|Hash->nil", shed.IndexFuncs{
		EncodeKey: func(fields shed.Item) (key []byte, err error) {
			key = make([]byte, 65)
			copy(key[:32], fields.BatchID)
			// key[32] = db.po(swarm.NewAddress(fields.Address))
			copy(key[33:], fields.Address)
			return key, nil
		},
		DecodeKey: func(key []byte) (e shed.Item, err error) {
			e.BatchID = key[:32]
			e.Address = key[33:65]
			return e, nil
		},
		EncodeValue: func(fields shed.Item) (value []byte, err error) {
			return nil, nil
		},
		DecodeValue: func(keyItem shed.Item, value []byte) (e shed.Item, err error) {
			return e, nil
		},
	})
	if err != nil {
		fmt.Printf("failed initializing index %v\n")
		return
	}

	postageIndexIndex, err := sh.NewIndex("BatchID|BatchIndex->Hash|Timestamp", shed.IndexFuncs{
		EncodeKey: func(fields shed.Item) (key []byte, err error) {
			key = make([]byte, 40)
			copy(key[:32], fields.BatchID)
			copy(key[32:40], fields.Index)
			return key, nil
		},
		DecodeKey: func(key []byte) (e shed.Item, err error) {
			e.BatchID = key[:32]
			e.Index = key[32:40]
			return e, nil
		},
		EncodeValue: func(fields shed.Item) (value []byte, err error) {
			value = make([]byte, 40)
			copy(value, fields.Address)
			copy(value[32:], fields.Timestamp)
			return value, nil
		},
		DecodeValue: func(keyItem shed.Item, value []byte) (e shed.Item, err error) {
			e.Address = value[:32]
			e.Timestamp = value[32:]
			return e, nil
		},
	})
	if err != nil {
		fmt.Printf("failed initializing index %v\n", err)
		return
	}

	fmt.Printf("Starting check for localstore at %s...\n", *path)

	var inconsistencies, corruptions []string

	inconsistencies = append(inconsistencies, checkIndexes(retrievalAccessIndex, retrievalDataIndex, "retrievalAccessIdx", "retrievalDataIndex")...)
	inconsistencies = append(inconsistencies, checkIndexes(pullIndex, retrievalDataIndex, "pullIdx", "retrievalDataIndex")...)
	inconsistencies = append(inconsistencies, checkIndexes(pushIndex, retrievalDataIndex, "pushIdx", "retrievalDataIndex")...)
	inconsistencies = append(inconsistencies, checkIndexes(gcIndex, retrievalDataIndex, "gcIdx", "retrievalDataIndex")...)
	inconsistencies = append(inconsistencies, checkIndexes(pinIndex, retrievalDataIndex, "pinIdx", "retrievalDataIndex")...)
	inconsistencies = append(inconsistencies, checkIndexes(postageChunksIndex, retrievalDataIndex, "postageChunksIdx", "retrievalDataIndex")...)
	inconsistencies = append(inconsistencies, checkIndexes(postageIndexIndex, retrievalDataIndex, "postageIndexIdx", "retrievalDataIndex")...)

	gcCnt, _ := gcSize.Get()
	rsvCnt, _ := reserveSize.Get()

	chunkCnt, _ := retrievalDataIndex.Count()

	if int(gcCnt+rsvCnt) > chunkCnt {
		inconsistencies = append(inconsistencies, fmt.Sprintf("gcSize+reserveSize(%d) > chunkCount(%d)", gcCnt+rsvCnt, chunkCnt))
	}

	err = retrievalDataIndex.Iterate(func(item shed.Item) (bool, error) {
		l, err := sharky.LocationFromBinary(item.Location)
		if err != nil {
			inconsistencies = append(inconsistencies, fmt.Sprintf("invalid sharky location for item %s err: %v", swarm.NewAddress(item.Address).String(), err))
			return false, nil
		}
		data := make([]byte, l.Length)
		err = sharkyStore.Read(context.Background(), l, data)
		if err != nil {
			inconsistencies = append(inconsistencies, fmt.Sprintf("cannot read location from sharky %v", err))
			return false, nil
		}
		ch := swarm.NewChunk(swarm.NewAddress(item.Address), data)
		if !cac.Valid(ch) && !soc.Valid(ch) {
			corruptions = append(corruptions, fmt.Sprintf("address %s", ch.Address().String()))
		}
		return false, nil
	}, nil)

	fmt.Printf("Check complete")
	if len(inconsistencies) > 0 {
		fmt.Printf("Found %d inconsistencies in indexes\n", len(inconsistencies))
		for _, v := range inconsistencies {
			fmt.Printf("INCONSISTENCY: %s\n", v)
		}
	}
	if len(corruptions) > 0 {
		fmt.Printf("Found %d data corruptions\n", len(corruptions))
		for _, v := range corruptions {
			fmt.Printf("DATA CORRUPTION: %s\n", v)
		}
	}

	if len(corruptions) == 0 && len(inconsistencies) == 0 {
		fmt.Println("No inconsistencies or corruptions found")
	}
}

func checkIndexes(
	srcIdx shed.Index,
	dstIdx shed.Index,
	srcStr, dstStr string,
) (msgs []string) {
	err := srcIdx.Iterate(func(item shed.Item) (bool, error) {
		if exists, err := dstIdx.Has(item); err != nil || !exists {
			addrStr := swarm.NewAddress(item.Address)
			msgs = append(msgs, fmt.Sprintf("item in %s and not in %s %s", srcStr, dstStr, addrStr.String()))
		}
		return false, nil
	}, nil)

	if err != nil {
		msgs = append(msgs, fmt.Sprintf("CRITICAL: failed checking %s index", srcStr))
	}

	return msgs
}
