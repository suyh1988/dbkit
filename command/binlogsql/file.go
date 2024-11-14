package binlogsql

import (
	"errors"
	"fmt"
	"github.com/rs/zerolog/log"
	"io/ioutil"
	"os"
	"regexp"
)

func CreateFile(fileName string) error {
	_, err := os.Stat(fileName)
	if os.IsNotExist(err) {
		log.Info().Msg(fmt.Sprintf("%s not exits.", fileName))
		// 文件不存在，创建文件
		file, err := os.Create(fileName)
		if err != nil {
			err = errors.New(fmt.Sprintf("创建文件失败:%s", fileName))
			log.Error().Err(err)
			return err
		}
		defer file.Close() // 确保在退出前关闭文件
		return nil
	} else if err != nil {
		// 其他错误
		err = errors.New(fmt.Sprintf("检查文件失败:%s", fileName))
		log.Error().Err(err)
		return err
	} else {
		// 文件已存在，清空文件
		file, err := os.OpenFile(fileName, os.O_WRONLY|os.O_TRUNC, 0644)
		if err != nil {
			err = errors.New(fmt.Sprintf("清空文件失败:%s", fileName))
			log.Error().Err(err)
			return err
		}
		defer file.Close()
		log.Info().Msg(fmt.Sprintf("%s 文件已存在并已清空.", fileName))
		return nil
	}

}

func AppendToFile(fileName, content string) error {
	// 以追加模式打开文件，如果文件不存在则创建
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("打开文件失败: %v", err)
	}
	defer file.Close()

	// 写入内容到文件
	if _, err := file.WriteString(content); err != nil {
		return fmt.Errorf("写入文件失败: %v", err)
	}

	return nil
}

func GetFileNameByDir(path string) ([]string, error) {
	var fileNames []string
	re := regexp.MustCompile(`^mysql-bin\.\d{6}$`)
	// 读取目录中的文件列表
	files, err := ioutil.ReadDir(path)
	if err != nil {
		log.Error().Err(err).Msg("get file name in path fail")
		return []string{""}, err
	}

	// 遍历文件列表，获取文件名
	for _, file := range files {
		if !file.IsDir() && re.MatchString(file.Name()) { // 排除子目录，只记录文件
			fileNames = append(fileNames, file.Name())
			//fmt.Printf("binlog file:%s\n", file.Name())
		}
	}
	return fileNames, nil
}
