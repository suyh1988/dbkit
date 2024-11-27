package conf

import (
	"fmt"
	"io/ioutil"

	"github.com/rs/zerolog/log"
	"gopkg.in/yaml.v2"
)

<<<<<<< HEAD
type RedisConfig struct {
	Mode       string `yaml:"mode"`
	Standalone struct {
		Addr     string `yaml:"addr"`
		Password string `yaml:"password"`
		DB       int    `yaml:"db"`
	} `yaml:"standalone"`
	Sentinel struct {
		MasterName string   `yaml:"masterName"`
		Addrs      []string `yaml:"addrs"`
		Password   string   `yaml:"password"`
		DB         int      `yaml:"db"`
	} `yaml:"sentinel"`
	Cluster struct {
		Addrs    []string `yaml:"addrs"`
		Password string   `yaml:"password"`
	} `yaml:"cluster"`
}

type MongoDBConfig struct {
	URI        string `yaml:"uri"`
	Database   string `yaml:"database"`
	Collection string `yaml:"collection"`
	Options    struct {
		MaxPoolSize      int `yaml:"maxPoolSize"`
		ConnectTimeoutMS int `yaml:"connectTimeoutMS"`
	} `yaml:"options"`
}

type ElasticsearchConfig struct {
	Hosts    []string `yaml:"hosts"`
	Username string   `yaml:"username"`
	Password string   `yaml:"password"`
	Index    string   `yaml:"index"`
	Version  int      `yaml:"version"`
	Timeout  int      `yaml:"timeout"`
}

type KafkaConfig struct {
	Brokers []string `yaml:"brokers"`
	Topic   string   `yaml:"topic"`
	GroupId string   `yaml:"groupId"`
	SASL    struct {
		Enabled   bool   `yaml:"enabled"`
		Mechanism string `yaml:"mechanism"`
		Username  string `yaml:"username"`
		Password  string `yaml:"password"`
	} `yaml:"sasl"`
	TLS struct {
		Enabled bool   `yaml:"enabled"`
		CACert  string `yaml:"caCert"`
	} `yaml:"tls"`
}

type MappingConfig struct {
	Database string `yaml:"database"`
	Tables   []struct {
		Table      string   `yaml:"table"`
		TargetName string   `yaml:"target_name"`
		Columns    []string `yaml:"columns"`
	} `yaml:"tables"`
}

type Source struct {
	ServerID int    `yaml:"serverId"`
	IP       string `yaml:"ip"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Mode     string `yaml:"mode"`
	Charset  string `yaml:"charset"`
	Pos      string `yaml:"pos"`
}

type Target struct {
	Type          string              `yaml:"type"`
	Redis         RedisConfig         `yaml:"redis"`
	MongoDB       MongoDBConfig       `yaml:"mongodb"`
	Elasticsearch ElasticsearchConfig `yaml:"elasticsearch"`
	Kafka         KafkaConfig         `yaml:"kafka"`
}

type Config struct {
	Source  `yaml:"source"`
	Target  `yaml:"target"`
	Mapping []MappingConfig `yaml:"mapping"`
}

func ReadConf(conFile string) (*Config, error) {
	var config Config
=======
type Config struct {
	Redis struct {
		IP       string `yaml:"ip"`
		Port     string `yaml:"port"`
		User     string `yaml:"user"`
		Password string `yaml:"password"`
		DB       int    `yaml:"db"`
	} `yaml:"redis"`
	MySQLSync struct {
		ServerID      int    `yaml:"server_id"`
		MySQLIP       string `yaml:"source_ip"`
		MysqlPort     int    `yaml:"source_port"`
		MysqlUser     string `yaml:"source_user"`
		MysqlPassword string `yaml:"source_password"`
		SyncMode      string `yaml:"sync_mode"`
		TargetType    string `yaml:"target_type"`
		DbName        string `yaml:"db_name"`
		TableName     string `yaml:"table_name"`
		Charset       string `yaml:"charset"`
		BinlogPos     string `yaml:"binlog_pos"`
	} `yaml:"mysql_sync"`
}

func ReadConf(conFile string) (*Config, error) {
	var config *Config
>>>>>>> 9a9af1027f37ad5c37dfde516c40aab107a75600

	// 读取 YAML 文件
	data, err := ioutil.ReadFile(conFile)
	if err != nil {
<<<<<<< HEAD
		log.Error().Err(err).Msg("读取配置文件失败")
		fmt.Printf("读取配置文件失败:%v", err)
		return nil, err
=======
		log.Error().Err(err).Msg(fmt.Sprintf("读取配置文件失败"))
		return config, err
>>>>>>> 9a9af1027f37ad5c37dfde516c40aab107a75600
	}

	// 解析 YAML 文件
	err = yaml.Unmarshal(data, &config)
	if err != nil {
<<<<<<< HEAD
		log.Error().Err(err).Msg("配置文件解析失败")
		return nil, err
	}

	return &config, nil
=======
		log.Error().Err(err).Msg(fmt.Sprintf("配置文件解析失败"))
		return config, err
	}

	return config, nil
>>>>>>> 9a9af1027f37ad5c37dfde516c40aab107a75600
}

func saveConfig(conFile string, config *Config) error {
	data, err := yaml.Marshal(config)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(conFile, data, 0644)
	if err != nil {
		return err
	}

	return nil
}

<<<<<<< HEAD
// 更新binlog_pos并保存到文件
=======
// updateBinlogPos 更新 binlog_pos 并保存到文件
>>>>>>> 9a9af1027f37ad5c37dfde516c40aab107a75600
func UpdateBinlogPos(conFile string, newPos string) error {
	config, err := ReadConf(conFile)
	if err != nil {
		return err
	}

<<<<<<< HEAD
	config.Source.Pos = newPos
=======
	config.MySQLSync.BinlogPos = newPos
>>>>>>> 9a9af1027f37ad5c37dfde516c40aab107a75600
	err = saveConfig(conFile, config)
	if err != nil {
		return err
	}

<<<<<<< HEAD
	//log.Info().Msg(fmt.Sprintf("sync to %s", newPos))
=======
>>>>>>> 9a9af1027f37ad5c37dfde516c40aab107a75600
	return nil
}
