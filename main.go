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
	TYPE_DIANXING uint8 = 1
	TYPE_LIANTONG uint8 = 2
	TYPE_YIDONG   uint8 = 3
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
	var k uint64
	k = 0

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
		for i := uint64(0); i <= max; i = i + uint64(config.Interval) {
			t := &PhoneMD5{
				PType:       kt,
				PhoneNumber: phoneNumber + i,
				PhoneMD5:    md5.Sum([]byte(strconv.Itoa(int(phoneNumber + i)))),
			}

			if i%uint64(config.BatchNum) == 0 {
				BatchPutPhoneMD5(t, store)
				k++

				if k%uint64(config.Interval) == 0 {
					fmt.Printf("kt: %d. Phone: %d, MD5: %x\n", kt, t.PhoneNumber, t.PhoneMD5)
				}

				if k%uint64(config.BatchNum) == 0 {
					k = 0
					err = store.BatchCommit()
					if err != nil {
						panic(err)
					}
					store.NewBatch()
				}
			}
		}
	}

	store.BatchCommit()
	fmt.Printf("type %d done====\n", kt)
}

func main() {
	config, err := getConfig("config.ini")
	if err != nil {
		panic(err)
	}

	store, err := leveldbstore.NewLevelDBStore(config.DBName)
	//store.NewBatch()

	var dianxing []string = []string{"174", "190", "193", "133", "149", "153", "162", "1700", "1701", "1702", "173", "177", "180", "181", "189", "191", "199"}
	var liantong []string = []string{"130", "131", "132", "140", "145", "146", "155", "156", "166", "167", "1704", "1707", "1708", "1709", "171", "175", "176", "185", "186"}
	var yidong []string = []string{"197", "134", "135", "136", "137", "138", "139", "147", "148", "150", "151", "152", "157", "158", "159", "165", "1703", "1705", "1706", "172", "178", "182", "183", "184", "187", "188", "195", "198"}

	all := make(map[uint8][]string)
	all[TYPE_DIANXING] = dianxing
	all[TYPE_LIANTONG] = liantong
	all[TYPE_YIDONG] = yidong

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
