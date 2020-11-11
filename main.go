package main

import (
	"context"
	"crypto/md5"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/unknwon/goconfig"
	"strconv"
	"time"
)

type DBConfig struct {
	ProjectDBUrl      string `json:"projectdb_url"`
	ProjectDBUser     string `json:"projectdb_user"`
	ProjectDBPassword string `json:"projectdb_password"`
	ProjectDBName     string `json:"projectdb_name"`
}

const (
	TYPE_DIANXING = 1
	TYPE_LIANTONG = 2
	TYPE_YIDONG   = 3
)

type PhoneMD5 struct {
	Id          uint32 `json:"id" db:"Id"`
	PType       uint32 `json:"pType db:"PType"`
	PhoneNumber uint64 `json:"phoneNumber db:"PhoneNumber"`
	PhoneMD5    string `json:"phoneMD5" db:"PhoneMD5"`
}

var DefSagaApiDB *SagaApiDB

type SagaApiDB struct {
	DB *sqlx.DB
}

func (this *SagaApiDB) InsertPhoneMD5(info *PhoneMD5) error {
	valueStr := fmt.Sprintf("('%d','%d','%s')", info.PType, info.PhoneNumber, info.PhoneMD5)
	strSql := `insert into tbl_phone_lib_md5 (PType,PhoneNumber,PhoneMD5) values` + valueStr
	_, err := this.DB.Exec(strSql)
	return err
}

func NewSagaApiDB(dbConfig *DBConfig) (*SagaApiDB, error) {
	dbx, dberr := sqlx.Open("mysql",
		dbConfig.ProjectDBUser+
			":"+dbConfig.ProjectDBPassword+
			"@tcp("+dbConfig.ProjectDBUrl+
			")/"+dbConfig.ProjectDBName+
			"?charset=utf8&parseTime=true")
	if dberr != nil {
		return nil, dberr
	}

	ctx, cf := context.WithTimeout(context.Background(), 10*time.Second)
	defer cf()

	err := dbx.PingContext(ctx)
	if err != nil {
		return nil, err
	}

	dbx.SetMaxIdleConns(256)

	return &SagaApiDB{
		DB: dbx,
	}, nil
}

func getConfig(configFile string) (*DBConfig, error) {
	cfg, err := goconfig.LoadConfigFile(configFile)
	if err != nil {
		return nil, err
	}
	userName, err := cfg.GetValue("mysql", "username")
	if err != nil {
		return nil, err
	}
	passwd, err := cfg.GetValue("mysql", "passwd")
	if err != nil {
		return nil, err
	}
	dbName, err := cfg.GetValue("mysql", "dbname")
	if err != nil {
		return nil, err
	}
	dbUrl, err := cfg.GetValue("mysql", "dburl")
	if err != nil {
		return nil, err
	}

	return &DBConfig{
		ProjectDBUrl:      dbUrl,
		ProjectDBUser:     userName,
		ProjectDBPassword: passwd,
		ProjectDBName:     dbName,
	}, nil
}

const MAX3 uint64 = 99999999
const MAX4 uint64 = 9999999

func main() {
	config, err := getConfig("config.ini")
	if err != nil {
		panic(err)
	}

	DefSagaApiDB, err := NewSagaApiDB(config)
	if err != nil {
		panic(err)
	}

	var dianxing []string = []string{"174", "190", "193", "133", "149", "153", "162", "1700", "1701", "1702", "173", "177", "180", "181", "189", "191", "199"}
	var liantong []string = []string{"130", "131", "132", "140", "145", "146", "155", "156", "166", "167", "1704", "1707", "1708", "1709", "171", "175", "176", "185", "186"}
	var yidong []string = []string{"197", "134", "135", "136", "137", "138", "139", "147", "148", "150", "151", "152", "157", "158", "159", "165", "1703", "1705", "1706", "172", "178", "182", "183", "184", "187", "188", "195", "198"}

	all := make(map[uint32][]string)
	all[TYPE_DIANXING] = dianxing
	all[TYPE_LIANTONG] = liantong
	all[TYPE_YIDONG] = yidong

	// total 11.

	for kt, yunyingshang := range all {
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
					PhoneMD5:    fmt.Sprintf("%x", md5.Sum([]byte(strconv.Itoa(int(phoneNumber+i))))),
				}

				if i%100 == 0 {
					fmt.Printf("kt: %d. Phone: %d, MD5: %s\n", kt, t.PhoneNumber, t.PhoneMD5)
				}

				err := DefSagaApiDB.InsertPhoneMD5(t)
				if err != nil {
					panic(err)
				}
			}
		}
	}
}
