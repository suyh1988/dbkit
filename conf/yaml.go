package conf

import (
	"fmt"
	"io/ioutil"

	"github.com/rs/zerolog/log"
	"gopkg.in/yaml.v2"
)

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

	// 读取 YAML 文件
	data, err := ioutil.ReadFile(conFile)
	if err != nil {
		log.Error().Err(err).Msg(fmt.Sprintf("读取配置文件失败"))
		return config, err
	}

	// 解析 YAML 文件
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		log.Error().Err(err).Msg(fmt.Sprintf("配置文件解析失败"))
		return config, err
	}

	return config, nil
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

// updateBinlogPos 更新 binlog_pos 并保存到文件
func UpdateBinlogPos(conFile string, newPos string) error {
	config, err := ReadConf(conFile)
	if err != nil {
		return err
	}

	config.MySQLSync.BinlogPos = newPos
	err = saveConfig(conFile, config)
	if err != nil {
		return err
	}

	return nil
}
