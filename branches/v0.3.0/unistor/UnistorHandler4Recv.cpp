#include "UnistorHandler4Recv.h"
#include "UnistorApp.h"

///master�仯�Ĵ�������
void UnistorHandler4Recv::doEvent(UnistorApp* pApp,
                                  UnistorTss* tss,
                                  CwxMsgBlock*& msg,
                                  CWX_UINT32 uiPoolIndex)
{
    if (EVENT_SEND_MSG == msg->event().getEvent()){ ///������Ϣ
        UnistorHandler4Recv* pHandler =((UnistorRecvThreadUserObj*)tss->getUserObj())->getConn(msg->event().getConnId());
        if (pHandler){
            pHandler->reply(msg, false);
            msg = NULL;
        }
    }else if (CwxEventInfo::CONN_CREATED == msg->event().getEvent()){///���ӽ���
        CwxAppChannel* channel = pApp->getRecvChannels()[uiPoolIndex];
        if (channel->isRegIoHandle(msg->event().getIoHandle())){
            CWX_ERROR(("Handler[%] is register, it's a big bug, stop.", msg->event().getIoHandle()));
            pApp->stop();
            return;
        }
        UnistorHandler4Recv* pHandler = new UnistorHandler4Recv(pApp,
            pApp->reactor()->getNextConnId(),
            uiPoolIndex,
            channel);
        CwxINetAddr  remoteAddr;
        CwxSockStream stream(msg->event().getIoHandle());
        stream.getRemoteAddr(remoteAddr);
        pHandler->m_unPeerPort = remoteAddr.getPort();
        if (remoteAddr.getHostIp(tss->m_szBuf2K, 2047)){
            pHandler->m_strPeerHost = tss->m_szBuf2K;
        }
        pHandler->setHandle(msg->event().getIoHandle());
        pHandler->m_tss = (UnistorTss*)CwxTss::instance();
        if (0 != pHandler->open()){
            CWX_ERROR(("Failure to register handler[%d] from:%s:%u", pHandler->getHandle(), pHandler->m_strPeerHost.c_str(), pHandler->m_unPeerPort));
            delete pHandler;
            return;
        }
    }else if (EVENT_ZK_CONF_CHANGE == msg->event().getEvent()){
        UnistorZkConf* pConf = NULL;
        memcpy(&pConf, msg->rd_ptr(), sizeof(pConf));
        if (tss->m_pZkConf){
            if (tss->m_pZkConf->m_ullVersion > pConf->m_ullVersion){///<���þɰ汾
                delete pConf;
            }else{///�����°汾
                delete tss->m_pZkConf;
                tss->m_pZkConf = pConf;
            }
        }else{///<�����°汾
            tss->m_pZkConf = pConf;
        }
        CWX_INFO(("UnistorHandler4Recv[thread:%u]: ZK config is changed. master_idc:%s, is_master_idc:%s, master_host:%s, is_master=%s, sync_host:%s",
            tss->m_uiThreadIndex,
            tss->getMasterIdc(),
            tss->isMasterIdc()?"yes":"no",
            tss->getMasterHost(),
            tss->isMaster()?"yes":"no",
            tss->getSyncHost()));

    }else if (EVENT_ZK_LOCK_CHANGE == msg->event().getEvent()){
        UnistorZkLock* pLock = NULL;
        memcpy(&pLock, msg->rd_ptr(), sizeof(pLock));
        if (tss->m_pZkLock){
            if (tss->m_pZkLock->m_ullVersion > pLock->m_ullVersion){///<���þɰ汾
                delete pLock;
            }else{///�����°汾
                delete tss->m_pZkLock;
                tss->m_pZkLock = pLock;
            }
        }else{///<�����°汾
            tss->m_pZkLock = pLock;
        }
        CWX_INFO(("UnistorHandler4Recv[thread:%u]: ZK config is changed. master_idc:%s, is_master_idc:%s, master_host:%s, is_master=%s, sync_host:%s",
            tss->m_uiThreadIndex,
            tss->getMasterIdc(),
            tss->isMasterIdc()?"yes":"no",
            tss->getMasterHost(),
            tss->isMaster()?"yes":"no",
            tss->getSyncHost()));

    }else{
        CWX_ERROR(("Unkwown event type:%d", msg->event().getEvent()));
    }
}

/**
@brief ��ʼ�����������ӣ�����Reactorע������
@param [in] arg �������ӵ�acceptor��ΪNULL
@return -1���������������ӣ� 0�����ӽ����ɹ�
*/
int UnistorHandler4Recv::open (void * arg){
	int ret = CwxAppHandler4Channel::open(arg);
    if (0 == ret){
		((UnistorRecvThreadUserObj*)m_tss->getUserObj())->addConn(m_uiConnId, this);
	}
	return ret;
}

/**
@brief ֪ͨ���ӹرա�
@return 1������engine���Ƴ�ע�᣻0����engine���Ƴ�ע�ᵫ��ɾ��handler��-1����engine�н�handle�Ƴ���ɾ����
*/
int UnistorHandler4Recv::onConnClosed(){
	((UnistorRecvThreadUserObj*)m_tss->getUserObj())->removeConn(m_uiConnId);

	return -1;
}

int UnistorHandler4Recv::onInput(){
	///������Ϣ
	int ret = CwxAppHandler4Channel::recvPackage(getHandle(),
		m_uiRecvHeadLen,
		m_uiRecvDataLen,
		m_szHeadBuf,
		m_header,
		m_recvMsgData);
	///���û�н�����ϣ�0����ʧ�ܣ�-1�����򷵻�
	if (1 != ret) return ret;
	///���յ�һ�����������ݰ�
	///��Ϣ����
	ret = recvMessage();
	///���û���ͷŽ��յ����ݰ����ͷ�
	if (m_recvMsgData) CwxMsgBlockAlloc::free(m_recvMsgData);
	this->m_recvMsgData = NULL;
	this->m_uiRecvHeadLen = 0;
	this->m_uiRecvDataLen = 0;
	return ret;
}

///0���ɹ���-1��ʧ��
int UnistorHandler4Recv::recvMessage()
{
	int ret = 0;
    do{
        if (!m_recvMsgData){///һ���հ�
            ret = UNISTOR_ERR_ERROR;
            strcpy(m_tss->m_szBuf2K, "msg is empty.");
            break;
        }
        if (!m_tss->m_pReader->unpack(m_recvMsgData->rd_ptr(), m_recvMsgData->length(), false)){
            ret = UNISTOR_ERR_ERROR;
            strcpy(m_tss->m_szBuf2K, m_tss->m_pReader->getErrMsg());
            break;
        }
        if (!m_bAuth && !checkAuth(m_tss)){
            ret = UNISTOR_ERR_ERROR;
            break;
        }
        if ((m_header.getMsgType() == UnistorPoco::MSG_TYPE_RECV_ADD)||
            (m_header.getMsgType() == UnistorPoco::MSG_TYPE_RECV_SET) ||
            (m_header.getMsgType() == UnistorPoco::MSG_TYPE_RECV_UPDATE)||
            (m_header.getMsgType() == UnistorPoco::MSG_TYPE_RECV_INC)||
            (m_header.getMsgType() == UnistorPoco::MSG_TYPE_RECV_DEL))
        {///�����д����
            if (m_pApp->getRecvWriteHandler()->isCanWrite()){
                relayWriteThread();
                return 0;
            }else if (UnistorHandler4Trans::m_bCanTrans){///ת����master
                relayTransThread(m_recvMsgData);
                m_recvMsgData = NULL;
                return 0;
            }
            ret = UNISTOR_ERR_ERROR;
            strcpy(m_tss->m_szBuf2K, "No master.");
        }else if (m_header.getMsgType() == UnistorPoco::MSG_TYPE_RECV_GET){
            ret =  getKv(m_tss);
            if (UNISTOR_ERR_SUCCESS == ret) return 0;///�Ѿ��ظ�
        }else if (m_header.getMsgType() == UnistorPoco::MSG_TYPE_RECV_GETS){
            ret = getKvs(m_tss);
            if (UNISTOR_ERR_SUCCESS == ret) return 0;///�Ѿ��ظ�
        }else if (m_header.getMsgType() == UnistorPoco::MSG_TYPE_RECV_LIST){
            ret = getList(m_tss);
            if (UNISTOR_ERR_SUCCESS == ret) return 0;///�Ѿ��ظ�
        }else if (m_header.getMsgType() == UnistorPoco::MSG_TYPE_RECV_EXIST){
            ret = existKv(m_tss);
            if (UNISTOR_ERR_SUCCESS == ret) return 0;///�Ѿ��ظ�
        }else if (m_header.getMsgType() == UnistorPoco::MSG_TYPE_RECV_AUTH){
            ret = UNISTOR_ERR_SUCCESS;
        }else{
            ret = UNISTOR_ERR_ERROR;
            CwxCommon::snprintf(m_tss->m_szBuf2K, 2047, "Invalid msg type:%d", m_header.getMsgType());
            return -1; ///��Ч��Ϣ���ͣ�ֱ�ӹر�����
        }
    }while(0);

    CwxMsgBlock* msg = packReplyMsg(m_tss,
        m_header.getTaskId(),
        m_header.getMsgType()+1,
        ret,
        0,
        0,
        m_tss->m_szBuf2K);
    if (!msg) return -1; ///�ر�����
    return reply(msg, false);
}

CwxMsgBlock* UnistorHandler4Recv::packReplyMsg(UnistorTss* tss,
                                               CWX_UINT32 uiTaskId,
                                               CWX_UINT16 unMsgType,
                                               int ret,
                                               CWX_UINT32 uiVersion,
                                               CWX_UINT32 uiFieldNum,
                                               char const* szErrMsg)
{
    CwxMsgBlock* msg = NULL;
    if (UNISTOR_ERR_SUCCESS != UnistorPoco::packRecvReply(tss->m_pWriter,
        msg,
        uiTaskId,
        unMsgType,
        ret,
        uiVersion,
        uiFieldNum,
        szErrMsg,
        tss->m_szBuf2K))
    {
        CWX_ERROR(("Failure to pack reply msg, err=%s", tss->m_szBuf2K));
        return NULL;
    }
    return msg;
}

///get kv..UNISTOR_ERR_SUCCESS���ɹ����������������
bool UnistorHandler4Recv::checkAuth(UnistorTss* pTss){
    if (!m_bAuth){
        CwxKeyValueItem const* pItem = NULL;
        char const* szUser=NULL;
        char const* szPasswd = NULL;
        szUser = "";
        pItem = pTss->m_pReader->getKey(UNISTOR_KEY_U);
        if (pItem) szUser = pItem->m_szData;
        //get passwd
        szPasswd = "";
        pItem = pTss->m_pReader->getKey(UNISTOR_KEY_P);
        if (pItem) szPasswd = pItem->m_szData;

        if (m_pApp->getConfig().getRecv().getUser().length()){
            if ((m_pApp->getConfig().getRecv().getUser() != szUser) ||
                (m_pApp->getConfig().getRecv().getPasswd() != szPasswd))
            {
                if (szUser){
                    strcpy(pTss->m_szBuf2K, "Invalid user name or passwd.");
                }else{
                    strcpy(pTss->m_szBuf2K, "No auth.");
                }
                return false;
            }
        }
        m_bAuth = true;
    }
    return true;
}

///0���ɹ���-1��ʧ��
int UnistorHandler4Recv::reply(CwxMsgBlock* msg, bool bCloseConn){
	///���ͻظ������ݰ�
	msg->send_ctrl().setMsgAttr(bCloseConn?CwxMsgSendCtrl::CLOSE_NOTICE:CwxMsgSendCtrl::NONE);
	if (!this->putMsg(msg))	{
		CWX_ERROR(("Failure to send msg to reciever, conn[%u]", getHandle()));
		CwxMsgBlockAlloc::free(msg);
		return -1;
		///�ر�����
	}
	return 0;
}


///exist kv..UNISTOR_ERR_SUCCESS���ɹ����������������
int UnistorHandler4Recv::existKv(UnistorTss* pTss){
    CwxKeyValueItem const* key;
    CwxKeyValueItem const* field = NULL;
    CwxKeyValueItem const* extra = NULL;
    bool bVersion = false;
    char const* szUser;
    char const* szPasswd;
    bool        bMaster=false;
    CWX_UINT32 uiVersion;
    CWX_UINT32 uiFieldNum = 0;
    int ret = 0;
    if (UNISTOR_ERR_SUCCESS != UnistorPoco::parseExistKey(pTss->m_pReader,
        m_recvMsgData,
        key,
        field,
        extra,
        bVersion,
        szUser,
        szPasswd,
        bMaster,
        pTss->m_szBuf2K))
    {
        return UNISTOR_ERR_ERROR;
    }

    if (key->m_uiDataLen >= UNISTOR_MAX_KEY_SIZE){
        CwxCommon::snprintf(pTss->m_szBuf2K, 2047, "Key is too long[%u], max[%u]", key->m_uiDataLen , UNISTOR_MAX_KEY_SIZE-1);
        return UNISTOR_ERR_ERROR;
    }

    if (bMaster){
        if (pTss->isMasterIdc()){///��master idc
            if (!pTss->isMaster()){///�Լ�����master
                if (UnistorHandler4Trans::m_bCanTrans){
                    CwxMsgBlock* msg = NULL;
                    if (UNISTOR_ERR_SUCCESS != UnistorPoco::packExistKey(pTss->m_pWriter,
                        *key,
                        field,
                        extra,
                        bVersion,
                        NULL,
                        NULL,
                        false,
                        pTss->m_szBuf2K))
                    {
                        return UNISTOR_ERR_ERROR;
                    }
                    msg = CwxMsgBlockAlloc::malloc(pTss->m_pWriter->getMsgSize());
                    memcpy(msg->wr_ptr(), pTss->m_pWriter->getMsg(), pTss->m_pWriter->getMsgSize());
                    msg->wr_ptr(pTss->m_pWriter->getMsgSize());
                    relayTransThread(msg);
                    return UNISTOR_ERR_SUCCESS;
                }
                strcpy(pTss->m_szBuf2K, "No master.");
                return UNISTOR_ERR_ERROR;
            }
        }else{
            strcpy(pTss->m_szBuf2K, "No master.");
            return UNISTOR_ERR_ERROR;
        }
    }
    ret = m_pApp->getStore()->isExist(pTss,
        *key,
        field,
        extra,
        uiVersion,
        uiFieldNum);
    if (1 == ret){
        do{
            pTss->m_pWriter->beginPack();
            pTss->m_pWriter->addKeyValue(UNISTOR_KEY_RET, UNISTOR_ERR_SUCCESS);
            pTss->m_pWriter->addKeyValue(UNISTOR_KEY_FN, uiFieldNum);
            if (bVersion) pTss->m_pWriter->addKeyValue(UNISTOR_KEY_V, uiVersion);
            pTss->m_pWriter->pack();
            CwxMsgHead head(0, 0, m_header.getMsgType() + 1, m_header.getTaskId(), pTss->m_pWriter->getMsgSize());
            CwxMsgBlock* msg = CwxMsgBlockAlloc::pack(head, pTss->m_pWriter->getMsg(), pTss->m_pWriter->getMsgSize());
            reply(msg, false);
            return UNISTOR_ERR_SUCCESS;
        }while(0);
    }else if (0 == ret){
        CwxCommon::snprintf(pTss->m_szBuf2K, 2047, "Key[%s] doesn't exists.", key->m_szKey);
        ret = UNISTOR_ERR_NEXIST;
    }else{
        ret = UNISTOR_ERR_ERROR;
    }
    return ret;
}

///get kv..UNISTOR_ERR_SUCCESS���ɹ����������������
int UnistorHandler4Recv::getKv(UnistorTss* pTss){
	CwxKeyValueItem const* key=NULL;
    CwxKeyValueItem const* field = NULL;
    CwxKeyValueItem const* extra = NULL;
    bool bVersion = false;
    char const* szUser;
    char const* szPasswd;
    bool        bMaster=false;
	bool bKeyValue = false;
    bool bKeyInfo = false;
    CWX_UINT32 uiVersion;
	CWX_UINT32 uiBufLen = 0;
    CWX_UINT32 uiFieldNum = 0;
	int ret = 0;
	char const* buf = NULL;
    if (UNISTOR_ERR_SUCCESS != UnistorPoco::parseGetKey(pTss->m_pReader,
        m_recvMsgData,
        key,
        field,
        extra,
        bVersion,
        szUser,
        szPasswd,
        bMaster,
        bKeyInfo,
        pTss->m_szBuf2K))
    {
        return UNISTOR_ERR_ERROR;
    }
    if (key->m_uiDataLen >= UNISTOR_MAX_KEY_SIZE){
        CwxCommon::snprintf(pTss->m_szBuf2K, 2047, "Key is too long[%u], max[%u]", key->m_uiDataLen , UNISTOR_MAX_KEY_SIZE-1);
        return UNISTOR_ERR_ERROR;
    }
    if (bMaster){
        if (pTss->isMasterIdc()){///��master idc
            if (!pTss->isMaster()){///�Լ�����master
                if (UnistorHandler4Trans::m_bCanTrans){
                    CwxMsgBlock* msg = NULL;
                    if (UNISTOR_ERR_SUCCESS != UnistorPoco::packGetKey(pTss->m_pWriter,
                        *key,
                        field,
                        extra,
                        bVersion,
                        NULL,
                        NULL,
                        false,
                        bKeyInfo,
                        pTss->m_szBuf2K))
                    {
                        return UNISTOR_ERR_ERROR;
                    }
                    msg = CwxMsgBlockAlloc::malloc(pTss->m_pWriter->getMsgSize());
                    memcpy(msg->wr_ptr(), pTss->m_pWriter->getMsg(), pTss->m_pWriter->getMsgSize());
                    msg->wr_ptr(pTss->m_pWriter->getMsgSize());
                    relayTransThread(msg);
                    return UNISTOR_ERR_SUCCESS;
                }
                strcpy(pTss->m_szBuf2K, "No master.");
                return UNISTOR_ERR_ERROR;
            }
        }else{
            strcpy(pTss->m_szBuf2K, "No master.");
            return UNISTOR_ERR_ERROR;
        }
    }
	ret = m_pApp->getStore()->get(pTss,
        *key,
        field,
        extra,
        buf,
        uiBufLen,
        bKeyValue,
        uiVersion,
        uiFieldNum,
        bKeyInfo);
	if (1 == ret){
		do{
			pTss->m_pWriter->beginPack();
			pTss->m_pWriter->addKeyValue(UNISTOR_KEY_RET, UNISTOR_ERR_SUCCESS);
            pTss->m_pWriter->addKeyValue(UNISTOR_KEY_D, buf, uiBufLen, bKeyValue);
            pTss->m_pWriter->addKeyValue(UNISTOR_KEY_FN, uiFieldNum);
			if (bVersion) pTss->m_pWriter->addKeyValue(UNISTOR_KEY_V, uiVersion);
			pTss->m_pWriter->pack();
			CwxMsgHead head(0, 0, m_header.getMsgType() + 1, m_header.getTaskId(), pTss->m_pWriter->getMsgSize());
			CwxMsgBlock* msg = CwxMsgBlockAlloc::pack(head, pTss->m_pWriter->getMsg(), pTss->m_pWriter->getMsgSize());
			reply(msg, false);
			return UNISTOR_ERR_SUCCESS;
		}while(0);
	}else if (0 == ret){
		CwxCommon::snprintf(pTss->m_szBuf2K, 2047, "Key[%s] doesn't exists.", key->m_szKey);
		ret = UNISTOR_ERR_NEXIST;
	}else{
		ret = UNISTOR_ERR_ERROR;
	}
	return ret;
}


///get kv..UNISTOR_ERR_SUCCESS���ɹ����������������
int UnistorHandler4Recv::getKvs(UnistorTss* pTss){
    CwxKeyValueItem const* field = NULL;
    CwxKeyValueItem const* extra = NULL;
    list<pair<char const*, CWX_UINT16> > keys;
    char const* szUser=NULL;
    char const* szPasswd=NULL;
    bool bMaster = false;
    bool bKeyInfo = false;
    char const* buf = NULL;
	CWX_UINT32 uiBufLen = 0;
    CWX_UINT32 uiKeyNum = 0;
	int ret = 0;
    if (UNISTOR_ERR_SUCCESS != UnistorPoco::parseGetKeys(pTss->m_pReader,
        pTss->m_pItemReader,
        m_recvMsgData,
        keys,
        uiKeyNum,
        field,
        extra,
        szUser,
        szPasswd,
        bMaster,
        bKeyInfo,
        pTss->m_szBuf2K))
    {
        return UNISTOR_ERR_ERROR;
    }
    if (UNISTOR_MAX_GETS_KEY_NUM < uiKeyNum){
        CwxCommon::snprintf(pTss->m_szBuf2K, 2048, "Too many key num, max=%u", UNISTOR_MAX_GETS_KEY_NUM);
        return UNISTOR_ERR_ERROR;
    }else if (0 == uiKeyNum){
        CwxCommon::snprintf(pTss->m_szBuf2K, 2048, "No keys");
        return UNISTOR_ERR_ERROR;
    }
    list<pair<char const*, CWX_UINT16> >::iterator iter = keys.begin();
    while(iter != keys.end()){
        if (iter->second >= UNISTOR_MAX_KEY_SIZE){
            CwxCommon::snprintf(pTss->m_szBuf2K, 2047, "Key[%s] is too long[%u], max[%u]", iter->first, iter->second , UNISTOR_MAX_KEY_SIZE-1);
            return UNISTOR_ERR_ERROR;
        }
    }

    if (bMaster){
        if (pTss->isMasterIdc()){///��master idc
            if (!pTss->isMaster()){///�Լ�����master
                if (UnistorHandler4Trans::m_bCanTrans){
                    CwxMsgBlock* msg = NULL;
                    if (UNISTOR_ERR_SUCCESS != UnistorPoco::packGetKeys(pTss->m_pWriter,
                        pTss->m_pItemWriter,
                        keys,
                        field,
                        extra,
                        NULL,
                        NULL,
                        false,
                        bKeyInfo,
                        pTss->m_szBuf2K))
                    {
                        return UNISTOR_ERR_ERROR;
                    }
                    msg = CwxMsgBlockAlloc::malloc(pTss->m_pWriter->getMsgSize());
                    memcpy(msg->wr_ptr(), pTss->m_pWriter->getMsg(), pTss->m_pWriter->getMsgSize());
                    msg->wr_ptr(pTss->m_pWriter->getMsgSize());
                    relayTransThread(msg);
                    return UNISTOR_ERR_SUCCESS;
                }
                strcpy(pTss->m_szBuf2K, "No master.");
                return UNISTOR_ERR_ERROR;
            }
        }else{
            strcpy(pTss->m_szBuf2K, "No master.");
            return UNISTOR_ERR_ERROR;
        }
    }
    ret = m_pApp->getStore()->gets(pTss, keys, field, extra, buf, uiBufLen, bKeyInfo);
    if (-1 == ret) return UNISTOR_ERR_ERROR;
    pTss->m_pWriter->beginPack();
    ret = UNISTOR_ERR_SUCCESS;
    pTss->m_pWriter->addKeyValue(UNISTOR_KEY_RET, ret);
    pTss->m_pWriter->addKeyValue(UNISTOR_KEY_D, buf, uiBufLen, true);
    pTss->m_pWriter->pack();
	CwxMsgHead head(0, 0, m_header.getMsgType() + 1, m_header.getTaskId(), pTss->m_pWriter->getMsgSize());
	CwxMsgBlock* msg = CwxMsgBlockAlloc::pack(head, pTss->m_pWriter->getMsg(), pTss->m_pWriter->getMsgSize());
	reply(msg, false);
	return UNISTOR_ERR_SUCCESS;
}

int UnistorHandler4Recv::getList(UnistorTss* pTss){
	CwxKeyValueItem key;
	bool bKeyValue = false;
	CwxKeyValueItem const* begin = NULL;
	CwxKeyValueItem const* end = NULL;
    CwxKeyValueItem const* field = NULL;
    CwxKeyValueItem const* extra = NULL;
    CWX_UINT16  unNum = 0;
	bool        bAsc= true;
    bool        bBegin=true;
    bool        bKeyInfo=false;
    char const* szUser= NULL;
    char const* szPasswd = NULL;
    CWX_UINT32 uiVersion = 0;
    bool bMaster = false;
	int ret = 0;
	char const* szData = NULL;
    CWX_UINT32 uiDataLen = 0;
    CWX_UINT16 unKeyLen = 0;
    char const* szKey = NULL; 
    if (UNISTOR_ERR_SUCCESS != (ret = UnistorPoco::parseGetList(pTss->m_pReader,
        m_recvMsgData,
        begin,
        end,
        unNum,
        field,
        extra,
        bAsc,
        bBegin,
        bKeyInfo,
        szUser,
        szPasswd,
        bMaster,
        pTss->m_szBuf2K)))
    {
        return ret;
    }
    if (begin && begin->m_uiDataLen >= UNISTOR_MAX_KEY_SIZE){
        CwxCommon::snprintf(pTss->m_szBuf2K, 2047, "Begin key is too long[%u], max[%u]", begin->m_uiDataLen , UNISTOR_MAX_KEY_SIZE-1);
        return UNISTOR_ERR_ERROR;
    }
    if (end && end->m_uiDataLen >= UNISTOR_MAX_KEY_SIZE){
        CwxCommon::snprintf(pTss->m_szBuf2K, 2047, "End key is too long[%u], max[%u]", end->m_uiDataLen , UNISTOR_MAX_KEY_SIZE-1);
        return UNISTOR_ERR_ERROR;
    }

    if (bMaster){
        if (pTss->isMasterIdc()){///��master idc
            if (!pTss->isMaster()){///�Լ�����master
                if (UnistorHandler4Trans::m_bCanTrans){
                    CwxMsgBlock* msg = NULL;
                    if (UNISTOR_ERR_SUCCESS != UnistorPoco::packGetList(pTss->m_pWriter,
                        begin,
                        end,
                        unNum,
                        field,
                        extra,
                        bAsc,
                        bBegin,
                        bKeyInfo,
                        NULL,
                        NULL,
                        false,
                        pTss->m_szBuf2K))
                    {
                        return UNISTOR_ERR_ERROR;
                    }
                    msg = CwxMsgBlockAlloc::malloc(pTss->m_pWriter->getMsgSize());
                    memcpy(msg->wr_ptr(), pTss->m_pWriter->getMsg(), pTss->m_pWriter->getMsgSize());
                    msg->wr_ptr(pTss->m_pWriter->getMsgSize());
                    relayTransThread(msg);
                    return UNISTOR_ERR_SUCCESS;
                }
                strcpy(pTss->m_szBuf2K, "No master.");
                return UNISTOR_ERR_ERROR;
            }
        }else{
            strcpy(pTss->m_szBuf2K, "No master.");
            return UNISTOR_ERR_ERROR;
        }
    }
    if (!unNum){
        unNum = UNISTOR_DEF_LIST_NUM;
    }else if (unNum > UNISTOR_MAX_LIST_NUM){
        unNum = UNISTOR_MAX_LIST_NUM;
    }
    UnistorStoreCursor cursor(begin?begin->m_szData:NULL, end?end->m_szData:NULL, bAsc, bBegin);
	ret = m_pApp->getStore()->createCursor(cursor, field, extra, pTss->m_szBuf2K);
	if (-1 == ret){
		ret = UNISTOR_ERR_ERROR;
    }else if (0 == ret){
        ret = UNISTOR_ERR_ERROR;
        strcpy(pTss->m_szBuf2K, "Not support cursor");
    }else{
		pTss->m_pItemWriter->beginPack();
		while(unNum){
			unKeyLen = sizeof(pTss->m_szStoreKey);
			ret = m_pApp->getStore()->next(pTss,
                cursor,
                szKey,
                unKeyLen, 
                szData,
                uiDataLen,
                bKeyValue,
                uiVersion,
                bKeyInfo);

			if (-1 == ret){
				ret = UNISTOR_ERR_ERROR;
				break;
			}else if (0 == ret){
				ret = UNISTOR_ERR_SUCCESS;
				break;
			}
            ret = UNISTOR_ERR_SUCCESS;
            pTss->m_pItemWriter->addKeyValue(szKey, unKeyLen, szData, uiDataLen, bKeyValue);
            if (pTss->m_pItemWriter->getMsgSize() > UNISTOR_MAX_KVS_SIZE){
                break;
            }
			unNum--;
		}
        pTss->m_pItemWriter->pack();
		m_pApp->getStore()->closeCursor(cursor);
		if (!unNum) ret = UNISTOR_ERR_SUCCESS;
        if (UNISTOR_ERR_SUCCESS == ret){
            pTss->m_pWriter->beginPack();
            pTss->m_pWriter->addKeyValue(UNISTOR_KEY_RET, (CWX_INT32)0);
            pTss->m_pWriter->addKeyValue(UNISTOR_KEY_D, pTss->m_pItemWriter->getMsg(), pTss->m_pItemWriter->getMsgSize(), true);
            pTss->m_pWriter->pack();
        }
	}
	if (UNISTOR_ERR_SUCCESS == ret){
		CwxMsgHead head(0, 0, m_header.getMsgType() + 1, m_header.getTaskId(), pTss->m_pWriter->getMsgSize());
		CwxMsgBlock* msg = CwxMsgBlockAlloc::pack(head, pTss->m_pWriter->getMsg(), pTss->m_pWriter->getMsgSize());
		reply(msg, false);
		return UNISTOR_ERR_SUCCESS;
	}
	pTss->m_pWriter->beginPack();
	pTss->m_pWriter->addKeyValue(UNISTOR_KEY_RET, ret);
	pTss->m_pWriter->addKeyValue(UNISTOR_KEY_ERR, pTss->m_szBuf2K, strlen(pTss->m_szBuf2K));
	pTss->m_pWriter->pack();
	CwxMsgHead head(0, 0, m_header.getMsgType() + 1, m_header.getTaskId(), pTss->m_pWriter->getMsgSize());
	CwxMsgBlock* msg = CwxMsgBlockAlloc::pack(head, pTss->m_pWriter->getMsg(), pTss->m_pWriter->getMsgSize());
	reply(msg, false);
	return UNISTOR_ERR_SUCCESS;
}

///����Ϣת����write�߳�
void UnistorHandler4Recv::relayWriteThread(){
    m_recvMsgData->event().setConnId(m_uiConnId);
    m_recvMsgData->event().setMsgHeader(m_header);
    m_recvMsgData->event().setHostId(m_uiThreadPosIndex);
    m_recvMsgData->event().setSvrId(UnistorApp::SVR_TYPE_RECV_WRITE);
    m_recvMsgData->event().setEvent(CwxEventInfo::RECV_MSG);
    m_pApp->getWriteTheadPool()->append(m_recvMsgData);
    m_recvMsgData = NULL;
}

///����Ϣת����transfer�߳�
void UnistorHandler4Recv::relayTransThread(CwxMsgBlock* msg){
    msg->event().setConnId(m_uiConnId);
    msg->event().setMsgHeader(m_header);
    msg->event().getMsgHeader().setDataLen(msg->length());
    msg->event().setHostId(m_uiThreadPosIndex);
    msg->event().setSvrId(UnistorApp::SVR_TYPE_TRANSFER);
    msg->event().setEvent(CwxEventInfo::RECV_MSG);
    if (m_pApp->getTransThreadPool()->append(msg) <= 1){
        m_pApp->getTransChannel()->notice();
    }
}
