#ifndef __UNISTOR_MACRO_H__
#define __UNISTOR_MACRO_H__


#include "CwxGlobalMacro.h"
#include "CwxType.h"
#include "CwxStl.h"
#include "CwxStlFunc.h"

CWINUX_USING_NAMESPACE


///通信的key定义
#define UNISTOR_KEY_ASC    "asc"   ///<升序
#define UNISTOR_KEY_BEGIN      "begin" ///<开始 
#define UNISTOR_KEY_C       "c"  ///<cache
#define UNISTOR_KEY_CHUNK "chunk"  ///<数据发送的chunk的key
#define UNISTOR_KEY_CRC32  "crc32" ///<crc32签名
#define UNISTOR_KEY_END    "end"   ///<结束值的key
#define UNISTOR_KEY_ERR  "err"     ///error的key
#define UNISTOR_KEY_E "e"          ///<expire的key
#define UNISTOR_KEY_D "d"           ///<数据的data的key
#define UNISTOR_KEY_F  "f"          ///<field
#define UNISTOR_KEY_FN "fn"         ///<field number
#define UNISTOR_KEY_G   "g"         ///<group,为key的分组
#define UNISTOR_KEY_I "i"           ///<information
#define UNISTOR_KEY_IB "ib"         ///<isbegin
#define UNISTOR_KEY_K "k"           ///<数据的key的key
#define UNISTOR_KEY_M      "m"      ///<message的key
#define UNISTOR_KEY_MT "mt"         ///<master
#define UNISTOR_KEY_MD5    "md5"    ///<md5签名的key
#define UNISTOR_KEY_MAX    "max"    ///<最大值
#define UNISTOR_KEY_MIN    "min"    ///<最小值
#define UNISTOR_KEY_N "n"           ///<number
#define UNISTOR_KEY_P "p"            ///<password
#define UNISTOR_KEY_RET  "ret"       ///<返回值的ret
#define UNISTOR_KEY_SEQ   "seq"      ///<数据同步的消息序列号
#define UNISTOR_KEY_SESSION "session" ///<数据同步的session
#define UNISTOR_KEY_SID "sid"        ///<数据变更的sid
#define UNISTOR_KEY_SIGN   "sign"    ///<数据更新的sign
#define UNISTOR_KEY_SUBSCRIBE  "subscribe" ///<订阅的key
#define UNISTOR_KEY_T "t"           ///<timestamp
#define UNISTOR_KEY_TYPE   "type"   ///<binlog的类型，也就是数据更新的消息类型
#define UNISTOR_KEY_U    "u"       ///<user
#define UNISTOR_KEY_ZIP    "zip"  ///<压缩标示
#define UNISTOR_KEY_V      "v"   ///<version key
#define UNISTOR_KEY_X      "x"  ///<引擎扩展key

///驱动的对象创建symbol name
#define UNISTOR_ENGINE_CREATE_SYMBOL_NAME  "unistor_create_engine"

///错误代码定义，0~100是系统级的错误
#define UNISTOR_ERR_SUCCESS          0  ///<成功
#define UNISTOR_ERR_ERROR            1 ///<无需区分的笼统错误
#define UNISTOR_ERR_FAIL_AUTH        2 ///<鉴权失败
#define UNISTOR_ERR_LOST_SYNC        3 ///<失去了同步状态
#define UNISTOR_ERR_NO_MASTER        4 ///<没有master

//100以上是应用及的错误
#define UNISTOR_ERR_NEXIST            101   ///<不存在
#define UNISTOR_ERR_EXIST             102   ///<存在
#define UNISTOR_ERR_VERSION           103   ///<版本错误
#define UNISTOR_ERR_OUTRANGE          104   ///<inc超出范围
#define UNISTOR_ERR_TIMEOUT           105   ///<超时

#define UNISTOR_TRANS_TIMEOUT_SECOND   5  ///<数据转发超时
#define UNISTOR_CONN_TIMEOUT_SECOND    3  ///<连接超时

#define UNISTOR_CLOCK_INTERANL_SECOND  1  ///<时钟间隔
#define UNISTOR_CHECK_ZK_LOCK_EXPIRE   10  ///<检测zk锁状况超时

#define UNISTOR_MAX_DATA_SIZE              2 * 1024 * 1024 ///<最大的DATA大小
#define UNISTOR_MAX_KEY_SIZE               1024            ///<最大的key的大小
#define UNISTOR_MAX_KV_SIZE				  (2 * 1024 * 1024 +  8 * 1024) ///<最大的data size
#define UNISTOR_DEF_KV_SIZE                (64 * 1024) ///<缺省的存储大小
#define UNISTOR_MAX_KVS_SIZE              (20 * 1024 * 1024) ///<最大返回数据包大小为20M
#define UNISTOR_MAX_CHUNK_KSIZE         (20 * 1024) ///<最大的chunk size
#define UNISTOR_ZIP_EXTRA_BUF           128
#define UNISTOR_DEF_LIST_NUM             50    ///<list的缺省数量
#define UNISTOR_MAX_LIST_NUM             1000   ///<list的最大数量
#define UNISTOR_MAX_BINLOG_FLUSH_COUNT  10000 ///<服务启动时，最大的skip sid数量
#define UNISTOR_KEY_START_VERION        1 ///<key的起始版本号
#define UNISTOR_MAX_GETS_KEY_NUM        1024 ///<最大的mget的key的数量

#define  UNISTOR_WRITE_CACHE_MBYTE          128  ///<128M的写cache
#define  UNISTOR_WRITE_CACHE_KEY_NUM        10000 ///<cache的最大记录数

#define  UNISTOR_DEF_SOCK_BUF_KB  64   ///<缺省的socket buf，单位为KB
#define  UNISTOR_MIN_SOCK_BUF_KB  4    ///<最小的socket buf，单位为KB
#define  UNISTOR_MAX_SOCK_BUF_KB  (8 * 1024) ///<最大的socket buf，单位为KB
#define  UNISTOR_DEF_CHUNK_SIZE_KB 64   ///<缺省的chunk大小，单位为KB
#define  UNISTOR_MIN_CHUNK_SIZE_KB  4   ///<最小的chunk大小，单位为KB
#define  UNISTOR_MAX_CHUNK_SIZE_KB  UNISTOR_MAX_CHUNK_KSIZE ///<最大的chunk大小，单位为KB
#define  UNISTOR_DEF_WRITE_CACHE_FLUSH_NUM  1000  ///<unistor存储触发flush的缺省数据变更数量
#define  UNISTOR_MIN_WRITE_CACHE_FLUSH_NUM  1     ///<unistor存储触发flush的最小数据变更数量
#define  UNISTOR_MAX_WRITE_CACHE_FLUSH_NUM  500000 ///<unistor存储触发flush的最大数据变更数量
#define  UNISTOR_DEF_WRITE_CACHE_FLUSH_SECOND  60  ///<unistor存储触发flush的缺省时间
#define  UNISTOR_MIN_WRITE_CACHE_FLUSH_SECOND  1     ///<unistor存储触发flush的最小时间
#define  UNISTOR_MAX_WRITE_CACHE_FLUSH_SECOND  1800  ///<unistor存储触发flush的最大时间

#define  UNISTOR_DEF_CONN_NUM  10            ///<数据同步缺省的并发连接数量
#define  UNISTOR_MIN_CONN_NUM  1            ///<数据同步最小的并发连接数量
#define  UNISTOR_MAX_CONN_NUM  256          ///<数据同步最大的并发连接数量 
#define  UNISTOR_DEF_EXPIRE_CONNCURRENT 32  ///<超时检查的缺省并发消息数量
#define  UNISTOR_MIN_EXPIRE_CONNCURRENT 1   ///<超时检查的最小并发消息数量
#define  UNISTOR_MAX_EXPIRE_CONNCURRENT 256 ///<超时检查的最大并发消息数量
#define  UNISTOR_CHECKOUT_INTERNAL   5      ///<存储引擎的checkpoint的间隔
#define  UNISTOR_PER_FETCH_EXPIRE_KEY_NUM 1024  ///<超时检查的一次获取key的数量
#define  UNISTOR_EXPORT_CONTINUE_SEEK_NUM  4096  ///<数据导出一次遍历的key的数量


#define  UNISTOR_REPORT_TIMEOUT  30  ///<report的超时时间
#define  UNISTOR_TRAN_AUTH_TIMEOUT 30  ///<转发认证的超时时间

#define  UNISTOR_MASTER_SWITCH_SID_INC  1000000  ///<unistor发送master切换，新master跳跃的sid的值




///开始写的函数，返回值：0，成功；-1：失败
typedef int (*UNISTOR_WRITE_CACHE_WRITE_BEGIN_FN)(void* context, char* szErr2K);
///写数据，返回值：0，成功；-1：失败
typedef int (*UNISTOR_WRITE_CACHE_WRITE_WRITE_FN)(void* context, char const* szKey, CWX_UINT16 unKeyLen, char const* szData, CWX_UINT32 uiDataLen, bool bDel, CWX_UINT32 ttOldExpire, char* szStoreKeyBuf, CWX_UINT16 unKeyBufLen, char* szErr2K);
///提交数据，返回值：0，成功；-1：失败
typedef int (*UNISTOR_WRITE_CACHE_WRITE_END_FN)(void* context, CWX_UINT64 ullSid, void* userData, char* szErr2K);
///key的相等比较函数。true：相等；false：不相等
typedef bool (*UNISTOR_KEY_CMP_EQUAL_FN)(char const* key1, CWX_UINT16 unKey1Len, char const* key2, CWX_UINT16 unKey2Len);
///key的less比较函数。0：相等；-1：小于；1：大于
typedef int (*UNISTOR_KEY_CMP_LESS_FN)(char const* key1, CWX_UINT16 unKey1Len, char const* key2, CWX_UINT16 unKey2Len);
///key的hash函数。
typedef size_t (*UNISTOR_KEY_HASH_FN)(char const* key1, CWX_UINT16 unKey1Len);
///key的group函数。
typedef CWX_UINT32 (*UNISTOR_KEY_GROUP_FN)(char const* key1, CWX_UINT16 unKey1Len);
///key的ascii级别less函数。0：相等；-1：小于；1：大于
typedef int (*UNISTOR_KEY_ASCII_LESS)(char const* key1, CWX_UINT16 unKey1Len, char const* key2, CWX_UINT16 unKey2Len);

#endif
