##### 简介
这是一个go语言开发的MySQL小工具，支持解析binlog，binlog文件分析，数据同步（目前支持数据同步到redis、mongodb）

##### 使用帮助
dbkit --help
NAME:
   mysql binlog tool - 1. 支持mysql binlog文件解析出sql以及回滚sql,需要在mysql服务器上执行,并且需要连上mysql;
        2. 支持分析binlog文件,还有哪些表有写入,通常用于下线检查;
        3. 支持mysql数据变更同步到异构数据库,如redis/mongodb/es等
USAGE:
   dbkit [global options] command [command options] [arguments...]

VERSION:
   1.0.0

COMMANDS:
   binlogsql  get sql or flash back from binlog
   sync       mysql sync data to other database
   filter     mysqldump file filter by database and table
   help, h    Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --runid value   Runid of this action. (default: 0)
   --type value    Action type.
   --debug         run with debug mode
   --config value  Global Config File
   --help, -h      show help
   --version, -v   print the version

#####子命令简介
目前支持3个子命令： binlogsql | sync | filter

###### binlogsql: 支持在线和离线的正向解析和反向解析MySQL的binlog，可以用来检查binlog和数据误删除恢复
NAME:
   dbkit binlogsql - get sql or flash back from binlog

USAGE:
   dbkit binlogsql [command options] [arguments...]

OPTIONS:
   --ip value         .
   --port value       
   --user value       master user name
   --password value   master user password
   --db value         master database name
   --table value      master table name
   --mode value       sql mode: flashback(restore sql); general(get binlog sql); stat(get binlog file statistics of write info) (default: "general")
   --serverid value   mysql server id (default: 8818)
   --charset value    mysql charset (default: "utf8mb4")
   --startFile value  
   --stopFile value   
   --startPose value  binlog start pose (default: 0)
   --stopPose value   binlog start pose (default: 0)
   --startTime value  binlog start start time
   --stopTime value   binlog start start time
   --output value     sql output file
   --stopNever value  keep running when read all binlog files (default: "false")
   --ddl value        including ddl sql (default: "false")
   --rotate value     show binlog file rotate event (default: "false")
   --binlogDir value  binlog file dir

###### sync: 支持从MySQL全量同步、增量同步 一个或多个表到redis、mongodb, 同步到其他类型数据库暂未开发
NAME:
   dbkit sync - mysql sync data to other database

USAGE:
   dbkit sync [command options] [arguments...]

OPTIONS:
   --conf value                    sync configuration file
   --rewrite_event_interval value  write position to configure file interval of event (default: 100)
   --rewrite_time_interval value   write position to configure file interval of time(second) (default: 30)
   --redis_write_mode value        write data to redis mode when full dump  (default: "batch")
   --write_batch_size value        write data to redis batch size when full dump (default: 1000)   
