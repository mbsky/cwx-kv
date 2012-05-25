#ifndef __UNISTOR_TSS_H__
#define __UNISTOR_TSS_H__


#include "UnistorMacro.h"
#include "CwxLogger.h"
#include "CwxTss.h"
#include "CwxPackageReader.h"
#include "CwxPackageWriter.h"
#include "UnistorDef.h"



///定义EVENT类型
#define EVENT_ZK_CONNECTED      (CwxEventInfo::SYS_EVENT_NUM + 1) ///<建立了与zk的连接
#define EVENT_ZK_EXPIRED        (CwxEventInfo::SYS_EVENT_NUM + 2) ///<zk的连接失效
#define EVENT_ZK_FAIL_AUTH      (CwxEventInfo::SYS_EVENT_NUM + 3) ///<zk的认证失败
#define EVENT_ZK_SUCCESS_AUTH   (CwxEventInfo::SYS_EVENT_NUM + 4) ///<zk的认证成功 
#define EVENT_ZK_CONF_CHANGE    (CwxEventInfo::SYS_EVENT_NUM + 5) ///<ZK的conf节点配置变化
#define EVENT_ZK_LOCK_CHANGE    (CwxEventInfo::SYS_EVENT_NUM + 6) ///<ZK的锁变化
#define EVENT_ZK_LOST_WATCH     (CwxEventInfo::SYS_EVENT_NUM + 7) ///<失去watch
#define EVENT_ZK_ERROR          (CwxEventInfo::SYS_EVENT_NUM + 8) ///<zk错误
#define EVENT_ZK_SET_SID        (CwxEventInfo::SYS_EVENT_NUM + 9) ///<设置zk的sid
#define EVENT_SEND_MSG          (CwxEventInfo::SYS_EVENT_NUM + 10) ///<发送消息
#define EVENT_STORE_MSG_START   (CwxEventInfo::SYS_EVENT_NUM + 100)  ///<存储的消息起始值
#define EVENT_STORE_COMMIT       EVENT_STORE_MSG_START ///<存储引擎执行了一次commit
#define EVENT_STORE_DEL_EXPIRE   (EVENT_STORE_MSG_START + 2)  ///<删除指定的超时key
#define EVENT_STORE_DEL_EXPIRE_REPLY (EVENT_STORE_MSG_START + 3) ///<删除指定的超时key的回复

///tss的用户线程数据对象
class UnistorTssUserObj{
public:
    ///构造函数
    UnistorTssUserObj(){}
    ///析构函数
    virtual ~UnistorTssUserObj(){}
};

///为存储engine预留的配置对象信息
class UnistorTssEngineObj{
public:
    ///构造函数
    UnistorTssEngineObj(){}
    ///析构函数
    virtual ~UnistorTssEngineObj(){}

};

//unistor的recv线程池的tss
class UnistorTss:public CwxTss{
public:
    ///构造函数
    UnistorTss():CwxTss(){
        m_pZkConf = NULL;
        m_pZkLock = NULL;
        m_pReader = NULL;
        m_pItemReader = NULL;
        m_pEngineReader = NULL;
        m_pEngineItemReader = NULL;
        m_pWriter = NULL;
        m_pItemWriter = NULL;
        m_pEngineWriter = NULL;
        m_pEngineItemWriter = NULL;
        m_engineConf = NULL;
        m_uiThreadIndex = 0;
        m_szDataBuf = NULL;
        m_uiDataBufLen = 0;
        m_szDataBuf1 = NULL;
        m_uiDataBufLen1 = 0;
        m_szDataBuf2 = NULL;
        m_uiDataBufLen2 = 0;
        m_szDataBuf3 = NULL;
        m_uiDataBufLen3 = 0;
        m_userObj = NULL;
        m_uiBinLogVersion = 0;
        m_uiBinlogType = 0;
        m_pBinlogData = NULL;
    }
    ///析构函数
    ~UnistorTss();
public:
    ///tss的初始化，0：成功；-1：失败
    int init(UnistorTssUserObj* pUserObj=NULL);
    ///获取用户的数据
    UnistorTssUserObj* getUserObj() { return m_userObj;}
    ///获取package的buf，返回NULL表示失败
    inline char* getBuf(CWX_UINT32 uiSize){
        if (m_uiDataBufLen < uiSize){
            delete [] m_szDataBuf;
            m_szDataBuf = new char[uiSize];
            m_uiDataBufLen = uiSize;
        }
        return m_szDataBuf;
    }
    ///获取package的buf，返回NULL表示失败
    inline char* getBuf1(CWX_UINT32 uiSize){
        if (m_uiDataBufLen1 < uiSize){
            delete [] m_szDataBuf1;
            m_szDataBuf1 = new char[uiSize];
            m_uiDataBufLen1 = uiSize;
        }
        return m_szDataBuf1;
    }
    ///获取package的buf，返回NULL表示失败
    inline char* getBuf2(CWX_UINT32 uiSize){
        if (m_uiDataBufLen2 < uiSize){
            delete [] m_szDataBuf2;
            m_szDataBuf2 = new char[uiSize];
            m_uiDataBufLen2 = uiSize;
        }
        return m_szDataBuf2;
    }
    ///获取package的buf，返回NULL表示失败
    inline char* getBuf3(CWX_UINT32 uiSize){
        if (m_uiDataBufLen3 < uiSize){
            delete [] m_szDataBuf3;
            m_szDataBuf3 = new char[uiSize];
            m_uiDataBufLen3 = uiSize;
        }
        return m_szDataBuf3;
    }
    ///是否是master idc
    bool isMasterIdc(){
        if (m_pZkConf && m_pZkConf->m_bMasterIdc) return true;
        return false;
    }
    ///获取master idc的名字
    char const* getMasterIdc() const{
        if (m_pZkConf && m_pZkConf->m_strMasterIdc.length()) return m_pZkConf->m_strMasterIdc.c_str();
        return "";
    }
    ///自己是否是master
    bool isMaster(){
        if (m_pZkLock && m_pZkLock->m_bMaster) return true;
        return false;
    }
    ///是否存在master
    bool isExistMaster(){
        if (m_pZkLock && m_pZkLock->m_strMaster.length()) return true;
        return false;
    }
    ///获取master的主机名
    char const* getMasterHost() const{
        if (m_pZkLock && m_pZkLock->m_strMaster.length()) return m_pZkLock->m_strMaster.c_str();
        return "";
    }
    ///获取idc内的上一个sync的主机名
    char const* getSyncHost() const{
        if (m_pZkLock){
            if (m_pZkLock->m_strPrev.length()) return m_pZkLock->m_strPrev.c_str();
            if (m_pZkLock->m_strMaster.length()) return m_pZkLock->m_strMaster.c_str();
        }
        return "";
    }

public:
    CWX_UINT32              m_uiBinLogVersion; ///<binlog的数据版本，用于binlog的分发
    CWX_UINT32              m_uiBinlogType;   ///<binlog的消息的类型，用于binlog的分发
    CwxKeyValueItem const*  m_pBinlogData; ///<binlog的data，用于binglog的分发

    UnistorZkConf*          m_pZkConf;  ///<zk的配置对象
    UnistorZkLock*          m_pZkLock;  ///<zk的锁信息
    CwxPackageReader*       m_pReader; ///<数据包的解包对象
    CwxPackageReader*       m_pItemReader; ///<数据包的解包对象
    CwxPackageReader*       m_pEngineReader; ///<engine使用的reader，外部不能使用
    CwxPackageReader*       m_pEngineItemReader; ///<engine使用的reader，存储引擎不能使用
    CwxPackageWriter*       m_pWriter; ///<数据包的pack对象
    CwxPackageWriter*       m_pItemWriter; ///<一个消息的数据包的pack对象
    CwxPackageWriter*       m_pEngineWriter; ///<engine使用的writer，外部不能使用
    CwxPackageWriter*       m_pEngineItemWriter; ///<engine使用的writer，外部不能使用
    char			        m_szStoreKey[UNISTOR_MAX_KEY_SIZE]; ///<存储的key
    CWX_UINT32              m_uiThreadIndex; ///<线程的索引号
    UnistorTssEngineObj*    m_engineConf;       ///<engine的conf信息，引擎可以使用
private:
    UnistorTssUserObj*    m_userObj;  ///用户的数据
    char*                  m_szDataBuf; ///<数据buf
    CWX_UINT32             m_uiDataBufLen; ///<数据buf的空间大小
    char*                  m_szDataBuf1; ///<数据buf1
    CWX_UINT32             m_uiDataBufLen1; ///<数据buf1的空间大小
    char*                  m_szDataBuf2; ///<数据buf2
    CWX_UINT32             m_uiDataBufLen2; ///<数据buf2的空间大小
    char*                  m_szDataBuf3; ///<数据buf3
    CWX_UINT32             m_uiDataBufLen3; ///<数据buf3的空间大小
};









#endif
