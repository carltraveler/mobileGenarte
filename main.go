package main

import (
	"crypto/md5"
	"fmt"
	"github.com/ontio/ontology/common"
	"github.com/ontio/ontology/core/store/leveldbstore"
	"github.com/unknwon/goconfig"
	"os"
	"os/signal"
	"strconv"
	"syscall"
)

type ServerConfig struct {
	BatchNum uint32
	DBName   string
	Interval uint32
}

const (
	TYPE_DIANXING  uint8 = 1
	TYPE_LIANTONG  uint8 = 2
	TYPE_YIDONG    uint8 = 3
	TYPE_GUANGDIAN uint8 = 4
)

type PhoneMD5 struct {
	PType       uint8
	PhoneNumber uint64
	PhoneMD5    [md5.Size]byte
}

func BatchPutPhoneMD5(info *PhoneMD5, store *leveldbstore.LevelDBStore) {
	sink := common.NewZeroCopySink(nil)
	sink.WriteUint8(info.PType)
	sink.WriteUint64(info.PhoneNumber)
	sink.WriteBytes(info.PhoneMD5[:])

	store.BatchPut(info.PhoneMD5[:], sink.Bytes())
}

func getConfig(configFile string) (*ServerConfig, error) {
	cfg, err := goconfig.LoadConfigFile(configFile)
	if err != nil {
		return nil, err
	}
	batchNumt, err := cfg.GetValue("config", "batchnum")
	if err != nil {
		return nil, err
	}

	batchNum, err := strconv.Atoi(batchNumt)
	if err != nil {
		return nil, err
	}

	intervalt, err := cfg.GetValue("config", "interval")
	if err != nil {
		return nil, err
	}

	interval, err := strconv.Atoi(intervalt)
	if err != nil {
		return nil, err
	}

	dbName, err := cfg.GetValue("config", "dbname")
	if err != nil {
		return nil, err
	}

	return &ServerConfig{
		Interval: uint32(interval),
		BatchNum: uint32(batchNum),
		DBName:   dbName,
	}, nil
}

const MAX3 uint64 = 99999999
const MAX4 uint64 = 9999999

func saveYunYingShang(passStore *leveldbstore.LevelDBStore, config *ServerConfig, kt uint8, yunyingshang []string) {
	var storeE leveldbstore.LevelDBStore
	storeE = *passStore
	store := &storeE
	store.NewBatch()
	fmt.Printf("saveYunYingShang handle type %d\n", kt)

	for _, prefix := range yunyingshang {
		var max uint64
		prefixInt, err := strconv.Atoi(prefix)
		if err != nil {
			fmt.Errorf("prefix: %s", prefix)
			panic(err)
		}

		if len(prefix) == 3 {
			max = MAX3
		} else {
			max = MAX4
		}

		phoneNumber := uint64(prefixInt) * (max + 1)
		fmt.Printf("type: %d. prefix int %d, start with prefix int %d\n", kt, uint64(prefixInt), phoneNumber)
		for i := uint64(0); i <= max; i++ {
			t := &PhoneMD5{
				PType:       kt,
				PhoneNumber: phoneNumber + i,
				PhoneMD5:    md5.Sum([]byte(strconv.Itoa(int(phoneNumber + i)))),
			}

			if i%uint64(config.Interval) == 0 {
				fmt.Printf("kt: %d. Phone: %d, MD5: %x\n", kt, t.PhoneNumber, t.PhoneMD5)
			}

			BatchPutPhoneMD5(t, store)
			if i%uint64(config.BatchNum) == 0 || i == max {
				err = store.BatchCommit()
				if err != nil {
					panic(err)
				}
				store.NewBatch()
			}
		}
	}
}

func main() {
	config, err := getConfig("config.ini")
	if err != nil {
		panic(err)
	}

	store, err := leveldbstore.NewLevelDBStore(config.DBName)
	//store.NewBatch()

	var liantong []string = []string{"196"}
	var guangdian []string = []string{"192"}

	all := make(map[uint8][]string)
	all[TYPE_LIANTONG] = liantong
	all[TYPE_GUANGDIAN] = guangdian

	// total 11.

	for kt, yunyingshang := range all {
		go saveYunYingShang(store, config, kt, yunyingshang)
	}

	fmt.Printf("all type start\n")
	waitToExit(store)
}

func waitToExit(store *leveldbstore.LevelDBStore) {
	exit := make(chan bool, 0)
	sc := make(chan os.Signal, 1)
	signal.Notify(sc, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	go func(store *leveldbstore.LevelDBStore) {
		for sig := range sc {
			fmt.Printf("waitToExit get sig====\n", sig.String())
			store.Close()
			close(exit)
			break
		}
	}(store)
	<-exit
}
