package filter

import (
	"errors"
	"example.com/m/v2/model"
	"github.com/rs/zerolog/log"
	"time"
)

func Run(options *model.DaemonOptions, _args []string) error {
	//参数检查
	if options.MysqlDumpFilter.InFile == "" || options.MysqlDumpFilter.OutFile == "" {
		return errors.New("The input parameter is incorrect. You need to enter the '--in/--out' parameter")
	}
	log.Info().Msgf("start filter %s to %s database and tables:%s", options.MysqlDumpFilter.InFile, options.MysqlDumpFilter.OutFile, options.MysqlDumpFilter.TableList)

	if options.MysqlDumpFilter.Mode == "buffer" {
		return FilterSQLByDatabasesBuffer(options)
	} else if options.MysqlDumpFilter.Mode == "line" {
		return FilterSQLByDatabasesSeq(options)
	} else if options.MysqlDumpFilter.Mode == "mmap" {
		// 创建用于同步的通道
		doneCh := make(chan struct{})
		errCh := make(chan error, 2) // 用于捕获错误

		// 并行运行解压任务
		go func() {
			defer close(doneCh) // 解压完成后通知
			log.Info().Msg("start decompressing")
			err := decompressGzipConcurrently(options)
			if err != nil {
				log.Error().Err(err).Msgf("解压失败")
				errCh <- err
			}
		}()

		log.Info().Msg("wait for decompress 60 seconds")
		time.Sleep(60 * time.Second)
		// 并行运行过滤任务
		go func() {
			// 等待解压开始
			<-doneCh
			log.Info().Msg("start filtering")
			err := filterFileUsingMmap(options)
			if err != nil {
				log.Error().Err(err).Msgf("过滤失败")
				errCh <- err
			}
		}()

		// 等待任务完成
		go func() {
			close(errCh) // 关闭错误通道
		}()

		// 检查错误
		for err := range errCh {
			if err != nil {
				log.Fatal().Err(err).Msg("任务执行失败")
			}
			return err
		}

		log.Info().Msg("所有任务执行完毕")
		return nil
	} else {
		return errors.New("option --mode input error, only line and buffer support")
	}
}
