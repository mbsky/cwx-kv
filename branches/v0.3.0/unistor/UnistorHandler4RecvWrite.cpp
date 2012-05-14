#include "UnistorHandler4RecvWrite.h"
#include "UnistorApp.h"
#include "UnistorHandler4Master.h"


///接收数据更新的消息,-1：处理失败，0：不处理此事件，1：处理此事件。
int UnistorHandler4RecvWrite::onRecvMsg(CwxMsgBlock*& msg, CwxTss* pThrEnv)
{
	UnistorTss* pTss = (UnistorTss*)pThrEnv;
	int ret = 0;
    CWX_INT64 llValue=0;
    CWX_UINT32 uiVersion=0;
    CWX_UINT32 uiFieldNum=0;
    if (m_bCanWrite){
        if (msg->event().getMsgHeader().getMsgType() == UnistorPoco::MSG_TYPE_RECV_ADD){
            ret = addKey(pTss, msg, uiVersion, uiFieldNum);
        }else if (msg->event().getMsgHeader().getMsgType() == UnistorPoco::MSG_TYPE_RECV_SET){
            ret =  setKey(pTss, msg, uiVersion, uiFieldNum);
        }else if (msg->event().getMsgHeader().getMsgType() == UnistorPoco::MSG_TYPE_RECV_UPDATE){
            ret =  updateKey(pTss, msg, uiVersion, uiFieldNum);
        }else if (msg->event().getMsgHeader().getMsgType() == UnistorPoco::MSG_TYPE_RECV_INC){
            ret =  incKey(pTss, msg, llValue, uiVersion);
        }else if (msg->event().getMsgHeader().getMsgType() == UnistorPoco::MSG_TYPE_RECV_DEL){
            ret =  delKey(pTss, msg, uiVersion, uiFieldNum);
        }else{
            ret = UNISTOR_ERR_ERROR;
            CwxCommon::snprintf(pTss->m_szBuf2K, 2048, "Unknown write msg type=%u", msg->event().getMsgHeader().getMsgType());
        }
    }else{
        ret = UNISTOR_ERR_NO_MASTER;
        strcpy(pTss->m_szBuf2K, "No master.");
    }

    CwxMsgBlock* block = NULL;

    if (msg->event().getMsgHeader().getMsgType() == UnistorPoco::MSG_TYPE_RECV_INC){
        ret = UnistorPoco::packRecvIncReply(pTss->m_pWriter,
            block,
            msg->event().getMsgHeader().getTaskId(),
            msg->event().getMsgHeader().getMsgType()+1,
            ret,
            llValue,
            uiVersion,
            pTss->m_szBuf2K,
            pTss->m_szBuf2K);
    }else{
        ret = UnistorPoco::packRecvReply(pTss->m_pWriter,
            block,
            msg->event().getMsgHeader().getTaskId(),
            msg->event().getMsgHeader().getMsgType()+1,
            ret,
            uiVersion,
            uiFieldNum,
            pTss->m_szBuf2K,
            pTss->m_szBuf2K);
    }

    if (UNISTOR_ERR_SUCCESS != ret){
        CWX_ERROR(("Failure to pack reply msg, err=%s, stopping....", pTss->m_szBuf2K));
        m_pApp->stop();
        return -1;
    }
    block->event().setConnId(msg->event().getConnId());
    block->event().setEvent(EVENT_SEND_MSG);
    block->event().setSvrId(UnistorApp::SVR_TYPE_RECV);
    block->send_ctrl().setMsgAttr(CwxMsgSendCtrl::NONE);
    if (m_pApp->getRecvThreadPools()[msg->event().getHostId()]->append(block)<=1){
        m_pApp->getRecvChannels()[msg->event().getHostId()]->notice();
    }
	return 1;
}

///return -1：处理失败，0：不处理此事件，1：处理此事件。
int UnistorHandler4RecvWrite::onTimeoutCheck(CwxMsgBlock*& msg, CwxTss* pThrEnv)
{
    UnistorTss* pTss = (UnistorTss*)pThrEnv;
    if (!m_bCanWrite){///如果不是master，则需要同步，调用同步方法的timecheck。
        m_pApp->getMasterHandler()->timecheck(pTss);
    }else{
        if (0 != m_pApp->getStore()->appendTimeStampBinlog(*pTss->m_pWriter, msg->event().getTimestamp(), pTss->m_szBuf2K)){
            CWX_ERROR(("Failure to append expire clock binlog, err=%s", pTss->m_szBuf2K));
        }else{
            m_pApp->getStore()->setExpireClock(msg->event().getTimestamp());
        }
    }
    return 1;
}


/// return -1：处理失败，0：不处理此事件，1：处理此事件。
int UnistorHandler4RecvWrite::onUserEvent(CwxMsgBlock*& msg, CwxTss* pThrEnv)
{
    UnistorTss* pTss = (UnistorTss*)pThrEnv;
    if (EVENT_ZK_CONF_CHANGE == msg->event().getEvent()){
        UnistorZkConf* pConf = NULL;
        memcpy(&pConf, msg->rd_ptr(), sizeof(pConf));
        if (pTss->m_pZkConf){
            if (pTss->m_pZkConf->m_ullVersion > pConf->m_ullVersion){///<采用旧版本
                delete pConf;
            }else{///采用新版本
                delete pTss->m_pZkConf;
                pTss->m_pZkConf = pConf;
            }
        }else{///<采用新版本
            pTss->m_pZkConf = pConf;
        }
        CWX_INFO(("UnistorHandler4RecvWrite: conf changed."));
    }else if (EVENT_ZK_LOCK_CHANGE == msg->event().getEvent()){
        UnistorZkLock* pLock = NULL;
        memcpy(&pLock, msg->rd_ptr(), sizeof(pLock));
        if (pTss->m_pZkLock){
            if (pTss->m_pZkLock->m_ullVersion > pLock->m_ullVersion){///<采用旧版本
                delete pLock;
            }else{///采用新版本
                delete pTss->m_pZkLock;
                pTss->m_pZkLock = pLock;
            }
        }else{///<采用新版本
            pTss->m_pZkLock = pLock;
        }
        CWX_INFO(("UnistorHandler4RecvWrite: lock changed."));
    }else if (msg->event().getEvent() >= EVENT_STORE_MSG_START){
        if (0 != m_pApp->getStore()->storeEvent(pTss, msg)){
            CWX_ERROR(("UnistorHandler4RecvWrite: failure to deal store event, err:%s", pTss->m_szBuf2K));
        }
        return 1;
    }else{
        CWX_ERROR(("UnistorHandler4RecvWrite: unknown event type:%u", msg->event().getEvent()));
        return 0;
    }    
    configChange(pTss);
    return 1;
}

void UnistorHandler4RecvWrite::configChange(UnistorTss* pTss){
    if (pTss->isMasterIdc() && pTss->isMaster()){
        if (!m_bCanWrite){
            CWX_UINT64 ullSid = m_pApp->getStore()->getBinLogMgr()->getMaxSid();
            ullSid += UNISTOR_MASTER_SWITCH_SID_INC;
            m_pApp->getStore()->setCurSid(ullSid);
        }
        m_bCanWrite = true;
    }else{
        m_bCanWrite = false;
    }
    CWX_INFO(("UnistorHandler4RecvWrite: ZK config is changed. master_idc:%s, is_master_idc:%s, master_host:%s, is_master=%s, sync_host:%s",
        pTss->getMasterIdc(),
        pTss->isMasterIdc()?"yes":"no",
        pTss->getMasterHost(),
        pTss->isMaster()?"yes":"no",
        pTss->getSyncHost()));
    m_pApp->getMasterHandler()->configChange(pTss);
}


///添加一个key。返回值：UNISTOR_ERR_SUCCESS：成功；其他：错误代码
int UnistorHandler4RecvWrite::addKey(UnistorTss* pTss,
                                    CwxMsgBlock* msg,
                                    CWX_UINT32& uiVersion,
                                    CWX_UINT32& uiFieldNum)
{
    CwxKeyValueItem const*  key=NULL;
    CwxKeyValueItem const*  field = NULL;
    CwxKeyValueItem const*  extra=NULL;
	CwxKeyValueItem const*  data = NULL;
	CWX_UINT32 uiExpire=0;
    CWX_UINT32 uiSign = 0;
    char const* szUser=NULL;
    char const* szPasswd=NULL;
    bool    bCache=true;
	int ret = UNISTOR_ERR_SUCCESS;
    ///解析数据包
    if (!pTss->m_pReader->unpack(msg->rd_ptr(), msg->length(), false)){
        ret = UNISTOR_ERR_ERROR;
        strcpy(pTss->m_szBuf2K, pTss->m_pReader->getErrMsg());
        return ret;
    }
	if (UNISTOR_ERR_SUCCESS != (ret = UnistorPoco::parseRecvAdd(pTss->m_pReader,
		key,
        field,
        extra,
		data,
		uiExpire,
        uiSign,
        uiVersion,
        bCache,
        szUser,
        szPasswd,
		pTss->m_szBuf2K)))
    {
		return ret;
	}
	if (key->m_uiDataLen >= UNISTOR_MAX_KEY_SIZE)	{
		CwxCommon::snprintf(pTss->m_szBuf2K, 2047, "Key is too long[%u], max[%u]", key->m_uiDataLen , UNISTOR_MAX_KEY_SIZE-1);
		return UNISTOR_ERR_ERROR;
	}
	if (data->m_uiDataLen > UNISTOR_MAX_DATA_SIZE){
		CwxCommon::snprintf(pTss->m_szBuf2K, 2047, "Data is too long[%u], max[%u]", data->m_uiDataLen , UNISTOR_MAX_DATA_SIZE);
		return UNISTOR_ERR_ERROR;
	}
    uiVersion = 0;
	ret = m_pApp->getStore()->addKey(pTss,
        *key,
        field,
        extra,
        *data,
        uiSign,
        uiVersion,
        uiFieldNum,
        bCache,
        uiExpire);
	if (1 == ret) return UNISTOR_ERR_SUCCESS;
    if (0 == ret) return UNISTOR_ERR_EXIST;
    return UNISTOR_ERR_ERROR;
}

///set一个key。返回值：UNISTOR_ERR_SUCCESS：成功；其他：错误代码
int UnistorHandler4RecvWrite::setKey(UnistorTss* pTss,
                                    CwxMsgBlock* msg,
                                    CWX_UINT32& uiVersion,
                                    CWX_UINT32& uiFieldNum)
{
	CwxKeyValueItem const* key=NULL;
    CwxKeyValueItem const* field = NULL;
    CwxKeyValueItem const* extra = NULL;
	CwxKeyValueItem const* data = NULL;
	CWX_UINT32 uiSign=0;
	CWX_UINT32 uiExpire = 0;
    bool bCache = true;
    char const* user=NULL;
    char const* passwd=NULL;
	int ret = UNISTOR_ERR_SUCCESS;
    ///解析数据包
    if (!pTss->m_pReader->unpack(msg->rd_ptr(), msg->length(), false)){
        ret = UNISTOR_ERR_ERROR;
        strcpy(pTss->m_szBuf2K, pTss->m_pReader->getErrMsg());
        return ret;
    }
	if (UNISTOR_ERR_SUCCESS != (ret = UnistorPoco::parseRecvSet(pTss->m_pReader,
		key,
        field,
        extra,
		data,
        uiSign,
		uiExpire,
        uiVersion,
        bCache,
        user,
        passwd,
		pTss->m_szBuf2K)))
    {
		return ret;
	}
	if (key->m_uiDataLen >= UNISTOR_MAX_KEY_SIZE){
		CwxCommon::snprintf(pTss->m_szBuf2K, 2047, "Key is too long[%u], max[%u]", key->m_uiDataLen , UNISTOR_MAX_KEY_SIZE-1);
		return UNISTOR_ERR_ERROR;
	}
	if (data->m_uiDataLen > UNISTOR_MAX_DATA_SIZE){
		CwxCommon::snprintf(pTss->m_szBuf2K, 2047, "Data is too long[%u], max[%u]", data->m_uiDataLen , UNISTOR_MAX_DATA_SIZE);
		return UNISTOR_ERR_ERROR;
	}
    uiVersion = 0;
	ret = m_pApp->getStore()->setKey(pTss,
        *key,
        field,
        extra,
        *data,
        uiSign,
        uiVersion,
        uiFieldNum,
        bCache,
        uiExpire);

	if (1 == ret) return UNISTOR_ERR_SUCCESS;
    if (0 == ret) return UNISTOR_ERR_NEXIST;

	return UNISTOR_ERR_ERROR;
}

///update一个key。返回值：UNISTOR_ERR_SUCCESS：成功；其他：错误代码
int UnistorHandler4RecvWrite::updateKey(UnistorTss* pTss,
                                       CwxMsgBlock* msg,
                                       CWX_UINT32& uiVersion,
                                       CWX_UINT32& uiFieldNum)
{
    CwxKeyValueItem const* key = NULL;
    CwxKeyValueItem const* field = NULL;
    CwxKeyValueItem const* extra = NULL;
	CwxKeyValueItem const* data = NULL;
	CWX_UINT32 uiExpire = 0;
    CWX_UINT32 uiSign=0;
    char const* user=NULL;
    char const* passwd=NULL;
	int ret = UNISTOR_ERR_SUCCESS;
    ///解析数据包
    if (!pTss->m_pReader->unpack(msg->rd_ptr(), msg->length(), false)){
        ret = UNISTOR_ERR_ERROR;
        strcpy(pTss->m_szBuf2K, pTss->m_pReader->getErrMsg());
        return ret;
    }
	if (UNISTOR_ERR_SUCCESS != (ret = UnistorPoco::parseRecvUpdate(pTss->m_pReader,
		key,
        field,
        extra,
		data,
        uiSign,
		uiExpire,
        uiVersion,
        user,
        passwd,
		pTss->m_szBuf2K)))
    {
		return ret;
	}
	if (key->m_uiDataLen >= UNISTOR_MAX_KEY_SIZE){
		CwxCommon::snprintf(pTss->m_szBuf2K, 2047, "Key is too long[%u], max[%u]", key->m_uiDataLen , UNISTOR_MAX_KEY_SIZE-1);
		return UNISTOR_ERR_ERROR;
	}
	if (data->m_uiDataLen > UNISTOR_MAX_DATA_SIZE){
		CwxCommon::snprintf(pTss->m_szBuf2K, 2047, "Data is too long[%u], max[%u]", data->m_uiDataLen , UNISTOR_MAX_DATA_SIZE);
		return UNISTOR_ERR_ERROR;
	}
    ret = m_pApp->getStore()->updateKey(pTss,
        *key,
        field,
        extra,
        *data,
        uiSign,
        uiVersion,
        uiFieldNum,
        uiExpire);
	if (1 == ret){
		return UNISTOR_ERR_SUCCESS;
	}else if (0 == ret){
		return UNISTOR_ERR_NEXIST;
	}else if (-2 == ret){
		return UNISTOR_ERR_VERSION;
	}
	return UNISTOR_ERR_ERROR;
}

///inc一个key的计数器。返回值：UNISTOR_ERR_SUCCESS：成功；其他：错误代码
int UnistorHandler4RecvWrite::incKey(UnistorTss* pTss,
                                    CwxMsgBlock* msg,
                                    CWX_INT64& llValue,
                                    CWX_UINT32& uiVersion)
{
    CwxKeyValueItem const* key = NULL;
    CwxKeyValueItem const* field = NULL;
    CwxKeyValueItem const* extra = NULL;
	CWX_INT64 num=0;
	CWX_INT64  llMax = 0;
	CWX_INT64  llMin = 0;
    CWX_UINT32  uiExpire=0;
    CWX_UINT32 uiSign = 0;
    char const* user=NULL;
    char const* passwd=NULL;
	int ret = UNISTOR_ERR_SUCCESS;
    ///解析数据包
    if (!pTss->m_pReader->unpack(msg->rd_ptr(), msg->length(), false)){
        ret = UNISTOR_ERR_ERROR;
        strcpy(pTss->m_szBuf2K, pTss->m_pReader->getErrMsg());
        return ret;
    }
	if (UNISTOR_ERR_SUCCESS != (ret = UnistorPoco::parseRecvInc(pTss->m_pReader,
		key,
        field,
        extra,
		num,
		llMax,
		llMin,
        uiExpire,
        uiSign,
        uiVersion,
        user,
        passwd,
		pTss->m_szBuf2K)))
    {
		return ret;
	}
	if (key->m_uiDataLen >= UNISTOR_MAX_KEY_SIZE){
		CwxCommon::snprintf(pTss->m_szBuf2K, 2047, "Key is too long[%u], max[%u]", key->m_uiDataLen , UNISTOR_MAX_KEY_SIZE-1);
		return UNISTOR_ERR_ERROR;
	}
	ret = m_pApp->getStore()->incKey(pTss,
        *key,
        field,
        extra,
        num,
        llMax,
        llMin,
        uiSign,
        llValue,
        uiVersion,
        uiExpire);
	if (1 == ret){
		return UNISTOR_ERR_SUCCESS;
	}else if (0 == ret){
		return UNISTOR_ERR_NEXIST;
    }else if (-2 == ret){
        return UNISTOR_ERR_VERSION;
	}else if (-3 == ret){
		return UNISTOR_ERR_OUTRANGE;
	}
	return UNISTOR_ERR_ERROR;
}

///delete一个key。返回值：UNISTOR_ERR_SUCCESS：成功；其他：错误代码
int UnistorHandler4RecvWrite::delKey(UnistorTss* pTss,
                                    CwxMsgBlock* msg,
                                    CWX_UINT32& uiVersion,
                                    CWX_UINT32& uiFieldNum)
{
    CwxKeyValueItem const* key=NULL;
    CwxKeyValueItem const* field = NULL;
    CwxKeyValueItem const* extra = NULL;
    char const* user=NULL;
    char const* passwd=NULL;
    int ret = UNISTOR_ERR_SUCCESS;
    ///解析数据包
    if (!pTss->m_pReader->unpack(msg->rd_ptr(), msg->length(), false)){
        ret = UNISTOR_ERR_ERROR;
        strcpy(pTss->m_szBuf2K, pTss->m_pReader->getErrMsg());
        return ret;
    }
	if (UNISTOR_ERR_SUCCESS != (ret = UnistorPoco::parseRecvDel(pTss->m_pReader,
		key,
        field,
        extra,
        uiVersion,
        user,
        passwd,
		pTss->m_szBuf2K)))
    {
		return ret;
	}
	if (key->m_uiDataLen >= UNISTOR_MAX_KEY_SIZE){
		CwxCommon::snprintf(pTss->m_szBuf2K, 2047, "Key is too long[%u], max[%u]", key->m_uiDataLen , UNISTOR_MAX_KEY_SIZE-1);
		return UNISTOR_ERR_ERROR;
	}
	ret = m_pApp->getStore()->delKey(pTss, *key, field, extra, uiVersion, uiFieldNum);
	if (1 == ret){
		return UNISTOR_ERR_SUCCESS;
	}else if (0 == ret){
		return UNISTOR_ERR_NEXIST;
	}
	return UNISTOR_ERR_ERROR;
}

