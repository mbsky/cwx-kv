#include "UnistorStoreBdb.h"

#define  BDB_MIN_READ_CACHE_MSIZE   128
#define  BDB_MAX_READ_CACHE_MSIZE   (64 * 1024)

#define  BDB_MIN_WRITE_CACHE_MSIZE   32
#define  BDB_MAX_WRITE_CACHE_MSIZE   1024

#define  BDB_MIN_READ_CACHE_KEY_NUM   100000
#define  BDB_MAX_READ_CACHE_KEY_NUM   800000000

extern "C"{
	UnistorStoreBase* unistor_create_engine()
	{
		return new UnistorStoreBdb();
	}
}

static int unistor_bdb_compare(DB *, const DBT *a, const DBT *b){
	char const* pa= (char const*)a->data;
	char const* pb= (char const*)b->data;
	int ret = memcmp(pa, pb, a->size<b->size?a->size:b->size);
	if (0 != ret) return ret;
	return a->size == b->size?0:(a->size < b->size?-1:1);
}

static int unistor_expire_compare(DB *, const DBT *a, const DBT *b){
    UnistorStoreExpireKey* pa= (UnistorStoreExpireKey*)a->data;
    UnistorStoreExpireKey* pb= (UnistorStoreExpireKey*)b->data;
    if (pa->m_ttExpire < pb->m_ttExpire) return -1;
    if (pa->m_ttExpire > pb->m_ttExpire) return 1;
    int ret = memcmp(pa->m_key, pb->m_key, a->size<b->size?a->size-sizeof(UnistorStoreExpireKey):b->size-sizeof(UnistorStoreExpireKey));
    if (0 != ret) return ret;
    return a->size == b->size?0:(a->size < b->size?-1:1);
}

//0:�ɹ���-1��ʧ��
int UnistorStoreBdb::parseConf(){
	string value;
	//get bdb:env_home
	if (!m_config->getConfFileCnf().getAttr("bdb", "env_home", value) || !value.length()){
		snprintf(m_szErrMsg, 2047, "Must set [bdb:env_home].");
		return -1;
	}
	m_bdbConf.m_strEnvPath = value;
	if ('/' != value[value.length()-1]) m_bdbConf.m_strEnvPath +="/";
	//get bdb:db_path
	if (!m_config->getConfFileCnf().getAttr("bdb", "db_path", value) || !value.length()){
		snprintf(m_szErrMsg, 2047, "Must set [bdb:db_path].");
		return -1;
	}
	m_bdbConf.m_strDbPath = value;
	if ('/' != value[value.length()-1]) m_bdbConf.m_strDbPath +="/";
	//get bdb:compress
	m_bdbConf.m_bZip = false;
	if (m_config->getConfFileCnf().getAttr("bdb", "compress", value)){
		if (value == "yes") m_bdbConf.m_bZip = true;
	}
	//get bdb:cache_msize
	m_bdbConf.m_uiCacheMByte = 512;
	if (m_config->getConfFileCnf().getAttr("bdb", "cache_msize", value)){
		m_bdbConf.m_uiCacheMByte = strtoul(value.c_str(), NULL, 10);
	}
	m_bdbConf.m_uiPageKSize = 0;
	if (m_config->getConfFileCnf().getAttr("bdb", "page_ksize", value)){
		m_bdbConf.m_uiPageKSize = strtoul(value.c_str(), NULL, 10);
		if (m_bdbConf.m_uiPageKSize > 64) m_bdbConf.m_uiPageKSize = 64;
	}
	CWX_INFO(("*****************begin bdb conf****************"));
	CWX_INFO(("env_home=%s", m_bdbConf.m_strEnvPath.c_str()));
	CWX_INFO(("db_path=%s", m_bdbConf.m_strDbPath.c_str()));
	CWX_INFO(("compress=%s", m_bdbConf.m_bZip?"yes":"no"));
	CWX_INFO(("cache_msize=%u", m_bdbConf.m_uiCacheMByte));
	CWX_INFO(("page_ksize=%u", m_bdbConf.m_uiPageKSize));
    CWX_INFO(("memcache_write_cache_msize=%u", m_uiWriteCacheMSize));
    CWX_INFO(("memcache_read_cache_msize=%u", m_uiReadCacheMSize));
    CWX_INFO(("memcache_read_cache_item=%u", m_uiReadCacheItemNum));
	CWX_INFO(("*****************end bdb conf****************"));
	return 0;
}


//���������ļ�.-1:failure, 0:success
int UnistorStoreBdb::init(UNISTOR_MSG_CHANNEL_FN msgPipeFunc,
                          void* msgPipeApp,
                          UnistorConfig const* config)
{
	int ret = 0;
	m_bValid = false;
	strcpy(m_szErrMsg, "Not init");
    if (0 != UnistorStoreBase::init(msgPipeFunc, msgPipeApp, config)) return -1;
    m_uiWriteCacheMSize = m_config->getCommon().m_uiWriteCacheMByte;
    if (m_uiWriteCacheMSize < BDB_MIN_WRITE_CACHE_MSIZE) m_uiWriteCacheMSize = BDB_MIN_WRITE_CACHE_MSIZE;
    if (m_uiWriteCacheMSize > BDB_MAX_WRITE_CACHE_MSIZE) m_uiWriteCacheMSize = BDB_MAX_WRITE_CACHE_MSIZE;

    m_uiReadCacheMSize = m_config->getCommon().m_uiReadCacheMByte;
    if (m_uiReadCacheMSize < BDB_MIN_READ_CACHE_MSIZE) m_uiReadCacheMSize = BDB_MIN_READ_CACHE_MSIZE;
    if (m_uiReadCacheMSize > BDB_MAX_READ_CACHE_MSIZE) m_uiReadCacheMSize = BDB_MAX_READ_CACHE_MSIZE;

    m_uiReadCacheItemNum = m_config->getCommon().m_uiReadCacheMaxKeyNum;
    if (m_uiReadCacheItemNum < BDB_MIN_READ_CACHE_KEY_NUM) m_uiReadCacheItemNum = BDB_MIN_READ_CACHE_KEY_NUM;
    if (m_uiReadCacheItemNum > BDB_MAX_READ_CACHE_KEY_NUM) m_uiReadCacheItemNum = BDB_MAX_READ_CACHE_KEY_NUM;
    //parse conf
    if (0 != parseConf()) return -1;
    //����cache
    if (0 != startCache(m_uiWriteCacheMSize,
        m_uiReadCacheMSize,
        m_uiReadCacheItemNum,
        UnistorStoreBdb::cacheWriteBegin,
        UnistorStoreBdb::cacheWrite,
        UnistorStoreBdb::cacheWriteEnd,
        this,
        UnistorStoreBdb::keyCmpEqual,
        UnistorStoreBdb::keyCmpLess,
        UnistorStoreBdb::keyCacheHash,
        32))
    {
        CWX_ERROR(("Failure to start cache"));
        return -1;
    }

    m_strEngine = m_config->getCommon().m_strStoreType;
	m_uiUncommitBinlogNum = 0;
    m_ullStoreSid = 0;
	m_bZip = m_bdbConf.m_bZip;

	//�򿪻���
    if ((ret = ::db_env_create(&m_bdbEnv, 0)) != 0){
		CwxCommon::snprintf(m_szErrMsg, 2047, "Failure to create bdb env, err-code=%d, err-msg=%s", ret, db_strerror(ret));
		CWX_ERROR((m_szErrMsg));
		return -1;
	}
	if ((ret = m_bdbEnv->set_cachesize(m_bdbEnv, m_bdbConf.m_uiCacheMByte/1024,
		(m_bdbConf.m_uiCacheMByte%1024)*1024*1024, 0)) != 0)
    {
		CwxCommon::snprintf(m_szErrMsg, 2047, "Failure to set bdb env cache, err-code=%d, err-msg=%s", ret, db_strerror(ret));
		CWX_ERROR((m_szErrMsg));
		return -1;
	}
	m_bdbEnv->set_data_dir(m_bdbEnv, m_bdbConf.m_strDbPath.c_str());
	m_bdbEnv->set_flags(m_bdbEnv, DB_TXN_NOSYNC, 1);
	m_bdbEnv->set_flags(m_bdbEnv, DB_TXN_WRITE_NOSYNC, 1);

	if ((ret = m_bdbEnv->open(m_bdbEnv, m_bdbConf.m_strEnvPath.c_str(),
		DB_RECOVER|DB_CREATE |DB_READ_UNCOMMITTED|DB_INIT_LOCK |DB_PRIVATE|DB_INIT_MPOOL | DB_INIT_TXN | DB_THREAD, 0644)) != 0)
    {
		CwxCommon::snprintf(m_szErrMsg, 2047, "Failure to create bdb env, home:%s, err-code=%d, err-msg=%s",
			m_bdbConf.m_strEnvPath.c_str(),
			ret,
			db_strerror(ret));
		CWX_ERROR((m_szErrMsg));
		return -1;
	}

    ///��ʼ�����ݵ�datase
	if ((ret = db_create(&m_bdb, m_bdbEnv, 0)) != 0){
		CwxCommon::snprintf(m_szErrMsg, 2047, "Failure to create bdb db, err-code=%d, err-msg=%s",
			ret,
			db_strerror(ret));
		CWX_ERROR((m_szErrMsg));
		return -1;
	}

	if (m_bdbConf.m_uiPageKSize){
		m_bdb->set_pagesize(m_bdb, m_bdbConf.m_uiPageKSize * 1024);
	}

	if (m_bdbConf.m_bZip){
		m_bdb->set_bt_compress(m_bdb, NULL, NULL);
	}
    ret = m_bdb->set_bt_compare(m_bdb, unistor_bdb_compare);
	if (0 != ret){
		CwxCommon::snprintf(m_szErrMsg, 2047, "Failure to set bdb's key compare function, err-code:%d, err-msg=%s",
			ret,
			db_strerror(ret));
		CWX_ERROR((m_szErrMsg));
		return -1;
	}
    ///��ʼ��ϵͳ��datase
    if ((ret = db_create(&m_sysDb, m_bdbEnv, 0)) != 0){
        CwxCommon::snprintf(m_szErrMsg, 2047, "Failure to create sys db, err-code=%d, err-msg=%s",
            ret,
            db_strerror(ret));
        CWX_ERROR((m_szErrMsg));
        return -1;
    }
    if (m_bdbConf.m_uiPageKSize){
        m_sysDb->set_pagesize(m_sysDb, m_bdbConf.m_uiPageKSize * 1024);
    }
    ///��ʼ��expire���ݿ�
    if (config->getCommon().m_bEnableExpire){
        if ((ret = db_create(&m_expireDb, m_bdbEnv, 0)) != 0){
            CwxCommon::snprintf(m_szErrMsg, 2047, "Failure to create expire db, err-code=%d, err-msg=%s",
                ret,
                db_strerror(ret));
            CWX_ERROR((m_szErrMsg));
            return -1;
        }
        if (m_bdbConf.m_uiPageKSize){
            m_expireDb->set_pagesize(m_expireDb, m_bdbConf.m_uiPageKSize * 1024);
        }
        ret = m_expireDb->set_bt_compare(m_expireDb, unistor_expire_compare);
        if (0 != ret){
            CwxCommon::snprintf(m_szErrMsg, 2047, "Failure to set expire's key compare function, err-code:%d, err-msg=%s",
                ret,
                db_strerror(ret));
            CWX_ERROR((m_szErrMsg));
            return -1;
        }
    }else{
        m_expireDb = NULL;
    }

	//open write thread transaction
	if ((ret = m_bdbEnv->txn_begin(m_bdbEnv, NULL, &m_bdbTxn, DB_READ_UNCOMMITTED)) != 0){
		CwxCommon::snprintf(m_szErrMsg, 2047, "Failure to begin transaction, err-code:%d, err-msg=%s",
			ret,
			db_strerror(ret));
		CWX_ERROR((m_szErrMsg));
		return -1;
	}
	/* Open a database with DB_BTREE access method. */
	if ((ret = m_bdb->open(m_bdb, m_bdbTxn, "unistor.db", NULL,
		DB_BTREE, DB_CREATE|DB_READ_UNCOMMITTED|DB_THREAD, 0644)) != 0)
    {
        m_bdbTxn->abort(m_bdbTxn);
        CwxCommon::snprintf(m_szErrMsg, 2047, "Failure to open bdb db[%unistor.db], err-code=%d, err-msg=%s",
            m_bdbConf.m_strDbPath.c_str(),
            ret,
            db_strerror(ret));
        CWX_ERROR((m_szErrMsg));
        return -1;
	}
    /* Open system database with DB_BTREE access method. */
    if ((ret = m_sysDb->open(m_sysDb, m_bdbTxn, "sys_unistor.db", NULL,
        DB_BTREE, DB_CREATE|DB_READ_UNCOMMITTED|DB_THREAD, 0644)) != 0)
    {
        m_bdbTxn->abort(m_bdbTxn);
        CwxCommon::snprintf(m_szErrMsg, 2047, "Failure to open bdb db[%sys_unistor.db], err-code=%d, err-msg=%s",
            m_bdbConf.m_strDbPath.c_str(),
            ret,
            db_strerror(ret));
        CWX_ERROR((m_szErrMsg));
        return -1;
    }
    if (m_expireDb){
        if ((ret = m_expireDb->open(m_expireDb, m_bdbTxn, "expire_unistor.db", NULL,
            DB_BTREE, DB_CREATE|DB_READ_UNCOMMITTED|DB_THREAD, 0644)) != 0)
        {
            m_bdbTxn->abort(m_bdbTxn);
            CwxCommon::snprintf(m_szErrMsg, 2047, "Failure to open bdb db[%expire_unistor.db], err-code=%d, err-msg=%s",
                m_bdbConf.m_strDbPath.c_str(),
                ret,
                db_strerror(ret));
            CWX_ERROR((m_szErrMsg));
            return -1;
        }
    }

	//open write thread transaction
	if ((ret = m_bdbTxn->commit(m_bdbTxn, 0)) != 0){
		CwxCommon::snprintf(m_szErrMsg, 2047, "Failure to commit transaction, err-code:%d, err-msg=%s",
			ret,
			db_strerror(ret));
		CWX_ERROR((m_szErrMsg));
		return -1;
	}
    m_bValid = true;
	//load sys key
    //open write thread transaction
    if ((ret = m_bdbEnv->txn_begin(m_bdbEnv, NULL, &m_bdbTxn, DB_READ_UNCOMMITTED)) != 0){
        m_bValid = false;
        CwxCommon::snprintf(m_szErrMsg, 2047, "Failure to begin transaction, err-code:%d, err-msg=%s",
            ret,
            db_strerror(ret));
        CWX_ERROR((m_szErrMsg));
        return -1;
    }
    ///��ȡ������Ϣ
    if (-1 == _loadSysInfo(m_bdbTxn, NULL)){
        m_bValid = false;
        return -1;
    }
    //open write thread transaction
    if ((ret = m_bdbTxn->commit(m_bdbTxn, 0)) != 0){
        m_bValid = false;
        CwxCommon::snprintf(m_szErrMsg, 2047, "Failure to commit transaction, err-code:%d, err-msg=%s",
            ret,
            db_strerror(ret));
        CWX_ERROR((m_szErrMsg));
        return -1;
    }
    m_bdbTxn = NULL;

	//�ָ�binlog
	if (0 != restore(m_ullStoreSid)){
		m_bValid = false;
		return -1;
	}
    //commit ����
    if (0 != commit(NULL)){
        m_bValid = false;
        return -1;
    }
	m_szErrMsg[0] = 0x00;
	return 0;
}

///��ʼд�ĺ���������ֵ��0���ɹ���-1��ʧ��
int UnistorStoreBdb::cacheWriteBegin(void* context, char* szErr2K)
{
    UnistorStoreBdb* pBdb = (UnistorStoreBdb*)context;
    if (!pBdb->m_bValid){
        if (szErr2K) strcpy(szErr2K, pBdb->m_szErrMsg);
        return -1;
    }
    CWX_ASSERT(!pBdb->m_bdbTxn);
    CWX_INFO(("Begin commit....................."));
    //flush binlog first
    if (0 != pBdb->flushBinlog(pBdb->m_szErrMsg)){
        if (szErr2K) strcpy(szErr2K, pBdb->m_szErrMsg);
        return -1;
    }
    int ret = 0;
    //open write thread transaction
    if ((ret = pBdb->m_bdbEnv->txn_begin(pBdb->m_bdbEnv, NULL, &pBdb->m_bdbTxn, DB_READ_UNCOMMITTED)) != 0){
        pBdb->m_bValid = false;
        CwxCommon::snprintf(pBdb->m_szErrMsg, 2047, "Failure to begin transaction, err-code:%d, err-msg=%s",
            ret,
            db_strerror(ret));
        CWX_ERROR((pBdb->m_szErrMsg));
        if (szErr2K) strcpy(szErr2K, pBdb->m_szErrMsg);
        pBdb->m_bdbTxn = NULL;
        return -1;
    }
    return 0;
}
///д���ݣ�����ֵ��0���ɹ���-1��ʧ��
int UnistorStoreBdb::cacheWrite(void* context,
                                char const* szKey,
                                CWX_UINT16 unKeyLen,
                                char const* szData,
                                CWX_UINT32 uiDataLen,
                                bool bDel,
                                CWX_UINT32 ttOldExpire,
                                char* szStoreKeyBuf,
                                CWX_UINT16 unKeyBufLen,
                                char* szErr2K)
{
    UnistorStoreBdb* pBdb = (UnistorStoreBdb*)context;
    if (!pBdb->m_bValid){
        if (szErr2K) strcpy(szErr2K, pBdb->m_szErrMsg);
        return -1;
    }
    CWX_ASSERT(pBdb->m_bdbTxn);
    UnistorStoreExpireKey* pKey=NULL;
    memcpy(szStoreKeyBuf, szKey, unKeyLen);
    if (bDel){
        if (0 != pBdb->_delBdbKey(pBdb->m_bdb, pBdb->m_bdbTxn, szStoreKeyBuf, unKeyLen, unKeyBufLen, 0, szErr2K)) return -1;
        if (pBdb->m_bEnableExpire){
            pKey = (UnistorStoreExpireKey*)szStoreKeyBuf;
            if (ttOldExpire){///ɾ����key
                pKey->m_ttExpire = ttOldExpire;
                memcpy(pKey->m_key, szKey, unKeyLen);
                if (0 != pBdb->_delBdbKey(pBdb->m_expireDb, pBdb->m_bdbTxn, szStoreKeyBuf, sizeof(UnistorStoreExpireKey) + unKeyLen, unKeyBufLen, 0, szErr2K)) return -1;
            }
        }
    }else{
        if (0 != pBdb->_setBdbKey(pBdb->m_bdb, pBdb->m_bdbTxn, szStoreKeyBuf, unKeyLen, unKeyBufLen, szData, uiDataLen, 0, szErr2K)) return -1;
        if (pBdb->m_bEnableExpire){
            pKey = (UnistorStoreExpireKey*)szStoreKeyBuf;
            CWX_UINT32 uiVersion=0;
            CWX_UINT32 ttNewExpire = 0;
            pBdb->getKvVersion(szData, uiDataLen, ttNewExpire, uiVersion);
            if (ttOldExpire != ttNewExpire){
                if (ttOldExpire){///ɾ����key
                    pKey->m_ttExpire = ttOldExpire;
                    memcpy(pKey->m_key, szKey, unKeyLen);
                    if (0 != pBdb->_delBdbKey(pBdb->m_expireDb, pBdb->m_bdbTxn, szStoreKeyBuf, sizeof(UnistorStoreExpireKey) + unKeyLen, unKeyBufLen, 0, szErr2K)) return -1;
                }
                pKey->m_ttExpire = ttNewExpire;
                memcpy(pKey->m_key, szKey, unKeyLen);
                if (0 != pBdb->_setBdbKey(pBdb->m_expireDb, pBdb->m_bdbTxn, szStoreKeyBuf, sizeof(UnistorStoreExpireKey) + unKeyLen, unKeyBufLen, "", 0, 0, szErr2K)) return -1;
            }
        }
    }
    return 0;
}

///�ύ���ݣ�����ֵ��0���ɹ���-1��ʧ��
int UnistorStoreBdb::cacheWriteEnd(void* context, CWX_UINT64 ullSid, char* szErr2K)
{
    int ret = 0;
    UnistorStoreBdb* pBdb = (UnistorStoreBdb*)context;
    if (!pBdb->m_bValid){
        if (szErr2K) strcpy(szErr2K, pBdb->m_szErrMsg);
        return -1;
    }
    CWX_ASSERT(pBdb->m_bdbTxn);
    //commit the bdb
    if (0 != pBdb->_updateSysInfo(pBdb->m_bdbTxn, ullSid, szErr2K)){
        return -1;
    }
    if ((ret = pBdb->m_bdbTxn->commit(pBdb->m_bdbTxn, 0)) != 0){
        pBdb->m_bValid = false;
        CwxCommon::snprintf(pBdb->m_szErrMsg, 2047, "Failure to commit bdb, err-code:%d, err-msg=%s",
            ret,
            db_strerror(ret));
        CWX_ERROR((pBdb->m_szErrMsg));
        if (szErr2K) strcpy(szErr2K, pBdb->m_szErrMsg);
        pBdb->m_bdbTxn = NULL;
        return -1;
    }
    pBdb->m_bdbTxn = NULL;
    CWX_INFO(("End commit....................."));
    return 0;
}


///����Ƿ����key��1�����ڣ�0�������ڣ�-1��ʧ��
int UnistorStoreBdb::isExist(UnistorTss* tss,
                             CwxKeyValueItem const& key,
                             CwxKeyValueItem const* field,
                             CwxKeyValueItem const* ,
                             CWX_UINT32& uiVersion,
                             CWX_UINT32& uiFieldNum)
{
	if (!m_bValid){
		strcpy(tss->m_szBuf2K, m_szErrMsg);
		return -1;
	}
    CWX_UINT32 ttOldExpire = 0;
    CWX_UINT32 uiBufLen = UNISTOR_MAX_KV_SIZE;
    char* szBuf = tss->getBuf(uiBufLen);
    int ret = _getKey(key.m_szData, key.m_uiDataLen, szBuf, uiBufLen, tss->m_szStoreKey, UNISTOR_MAX_KEY_SIZE, true, tss->m_szBuf2K);
    uiFieldNum = 0;
    if (1 == ret){
        ret = unpackFields(*tss->m_pEngineReader, szBuf, uiBufLen, ttOldExpire, uiVersion);
        if (-1 == ret){
            strcpy(tss->m_szBuf2K, tss->m_pEngineReader->getErrMsg());
            return -1;
        }
        ///����Ƿ�ʱ
        if (m_config->getCommon().m_bEnableExpire && (ttOldExpire <= m_ttExpireClock)) return 0;
        ///����Key/Value�ṹ
        if (0 == ret){
            if (field) return 0;
            return 1;
        }
        uiFieldNum = tss->m_pEngineReader->getKeyNum();
        if (field)  return tss->m_pEngineReader->getKey(field->m_szData)?1:0;
        return 1;
    }
    if (0 == ret)  return 0;
    return -1;
}

///����key��1���ɹ���0�����ڣ�-1��ʧ�ܣ�
int UnistorStoreBdb::addKey(UnistorTss* tss,
                            CwxKeyValueItem const& key,
                            CwxKeyValueItem const* field,
                            CwxKeyValueItem const* extra,
                            CwxKeyValueItem const& data,
                            CWX_UINT32 uiSign,
                            CWX_UINT32& uiVersion,
                            CWX_UINT32& uiFieldNum,
                            bool bCache,
                            CWX_UINT32 uiExpire)
{
    int iDataFieldNum=0;
    if (!m_bValid){
        strcpy(tss->m_szBuf2K, m_szErrMsg);
        return -1;
    }
    if (data.m_bKeyValue){
        if (-1 == (iDataFieldNum = CwxPackage::getKeyValueNum(data.m_szData, data.m_uiDataLen))){
            strcpy(tss->m_szBuf2K, "The data is not valid key/value structure.");
            return -1;
        }
    }
    bool bNewKeyValue = false;
    CWX_UINT32 uiBufLen = UNISTOR_MAX_KV_SIZE;
    char* szBuf = tss->getBuf(uiBufLen);
    CWX_UINT32 ttOldExpire = 0;
    CWX_UINT32 ttNewExpire = 0;
    CWX_UINT32 uiOldVersion = 0;
    uiFieldNum = 0;
    ///�޸�sign
    if (uiSign > 2) uiSign = 0;
    int ret = _getKey(key.m_szData, key.m_uiDataLen, szBuf, uiBufLen, tss->m_szStoreKey, UNISTOR_MAX_KEY_SIZE, bCache, tss->m_szBuf2K);
    if (-1 == ret) return -1;
    if (1 == ret) getKvVersion(szBuf, uiBufLen, ttOldExpire, uiOldVersion);
    ///���㳬ʱʱ��
    if (m_config->getCommon().m_bEnableExpire){
        if((1==ret) && (ttOldExpire<=m_ttExpireClock)) ret = 0;///��ʱ
        if (1 == ret){
            ttNewExpire = ttOldExpire;
        }else{
            ttNewExpire = getNewExpire(uiExpire);
        }
    }else{
        ttNewExpire = 0;
    }
    if (0 == ret){//not exist
        if (1 == uiSign){///����field������key�������
            strcpy(tss->m_szBuf2K, "Key doesn't exist, you can't add any field.");
            return -1; ///���ܵ�������field
        }
        if (field){
            tss->m_pEngineWriter->beginPack();
            if (!tss->m_pEngineWriter->addKeyValue(field->m_szData, field->m_uiDataLen, data.m_szData, data.m_uiDataLen, data.m_bKeyValue)){
                strcpy(tss->m_szBuf2K, tss->m_pEngineWriter->getErrMsg());
                return -1;
            }
            tss->m_pEngineWriter->pack();
            if (tss->m_pEngineWriter->getMsgSize() > UNISTOR_MAX_DATA_SIZE - getKvDataSignLen()){
                CwxCommon::snprintf(tss->m_szBuf2K, 2047, "Key/value is too big, max is [%u], now[%u]", UNISTOR_MAX_DATA_SIZE - getKvDataSignLen(), tss->m_pEngineWriter->getMsgSize());
                return -1;
            }
            uiBufLen = tss->m_pEngineWriter->getMsgSize();
            memcpy(szBuf, tss->m_pEngineWriter->getMsg(), uiBufLen);
            bNewKeyValue = true;
            uiFieldNum = 1;
        }else{
            if (data.m_uiDataLen > UNISTOR_MAX_DATA_SIZE - getKvDataSignLen()){
                CwxCommon::snprintf(tss->m_szBuf2K, 2047, "Key/value is too big, max is [%u], now[%u]", UNISTOR_MAX_DATA_SIZE - getKvDataSignLen(), data.m_uiDataLen);
                return -1;
            }
            uiBufLen = data.m_uiDataLen;
            memcpy(szBuf, data.m_szData, uiBufLen);
            bNewKeyValue = data.m_bKeyValue;
            uiFieldNum = iDataFieldNum;
        }
        if (!uiVersion) uiVersion = UNISTOR_KEY_START_VERION; ///��ʼ�汾
    }else if (1 == ret){//key����
        if (!uiVersion){
            uiVersion = uiOldVersion + 1;
        }
        if (0 == uiSign){//����key
            strcpy(tss->m_szBuf2K, "Key exists.");
            return 0; //key���ڡ�
        }
        bool bOldKv = isKvData(szBuf, uiBufLen);
        if (!bOldKv){///ԭ�����ݲ���key/value�ṹ����������β���add key��add field
            strcpy(tss->m_szBuf2K, "Key is not key/value, can't add field.");
            return 0; //key���ڡ�
        }
        if (!field && !data.m_bKeyValue){
            strcpy(tss->m_szBuf2K, "The added content is not field.");
            return 0; //key���ڡ�
        }
        //��ʱ��key���¡���valueһ��Ϊkey/value�ṹ
        ret = mergeAddKeyField(tss->m_pEngineWriter,
            tss->m_pEngineReader,
            tss->m_pEngineItemReader,
            key.m_szData,
            field,
            szBuf,
            uiBufLen-getKvDataSignLen(),
            bOldKv,
            data.m_szData,
            data.m_uiDataLen,
            data.m_bKeyValue,
            uiFieldNum,
            tss->m_szBuf2K);
        if (1 != ret) return ret;
        if (tss->m_pEngineWriter->getMsgSize() > UNISTOR_MAX_DATA_SIZE - getKvDataSignLen()){
            CwxCommon::snprintf(tss->m_szBuf2K, 2047, "Key/value is too big, max is [%u], now[%u]", UNISTOR_MAX_DATA_SIZE - getKvDataSignLen(), tss->m_pEngineWriter->getMsgSize());
            return -1;
        }
        uiBufLen = tss->m_pEngineWriter->getMsgSize();
        memcpy(szBuf, tss->m_pEngineWriter->getMsg(), uiBufLen);
        bNewKeyValue = true;
    }

    if (0 != appendAddBinlog(*tss->m_pEngineWriter,
        *tss->m_pEngineItemWriter,
        getKeyGroup(key.m_szData, key.m_uiDataLen),
        key,
        field,
        extra,
        data,
        uiExpire,
        uiSign,
        uiVersion,
        bCache,
        tss->m_szBuf2K))
    {
        m_bValid = false;
        strcpy(m_szErrMsg, tss->m_szBuf2K);
        return -1;
    }
    m_ullStoreSid = getCurSid();
    setKvDataSign(szBuf, uiBufLen, ttNewExpire, uiVersion, bNewKeyValue);
    if (0 != _setKey(key.m_szData, key.m_uiDataLen, szBuf, uiBufLen, ttOldExpire, bCache, tss->m_szBuf2K)) return -1;
    return 1;
}
                            
///����key��1���ɹ���0�����ڣ�-1��ʧ�ܣ�
int UnistorStoreBdb::syncAddKey(UnistorTss* tss,
                                CwxKeyValueItem const& key,
                                CwxKeyValueItem const* field,
                                CwxKeyValueItem const* extra,
                                CwxKeyValueItem const& data,
                                CWX_UINT32 uiSign,///<not use
                                CWX_UINT32 uiVersion,
                                bool bCache,
                                CWX_UINT32 uiExpire,
                                CWX_UINT64 ullSid,
                                bool  bRestore)
{
    return syncSetKey(tss, key, field, extra, data, uiSign, uiVersion, bCache, uiExpire, ullSid, bRestore);
}

///set key��1���ɹ���-1������0�������ڣ���������һ��key��fieldʱ��
int UnistorStoreBdb::setKey(UnistorTss* tss,
                            CwxKeyValueItem const& key,
                            CwxKeyValueItem const* field,
                            CwxKeyValueItem const* extra,
                            CwxKeyValueItem const& data,
                            CWX_UINT32 uiSign,
                            CWX_UINT32& uiVersion,
                            CWX_UINT32& uiFieldNum,
                            bool bCache,
                            CWX_UINT32 uiExpire)
{
    int iDataFieldNum=0;
    if (!m_bValid){
        strcpy(tss->m_szBuf2K, m_szErrMsg);
        return -1;
    }
    if (data.m_bKeyValue){
        if (-1 == (iDataFieldNum = CwxPackage::getKeyValueNum(data.m_szData, data.m_uiDataLen))){
            strcpy(tss->m_szBuf2K, "The data is not valid key/value structure.");
            return -1;
        }
    }
	bool bNewKeyValue = false;
    CWX_UINT32 ttOldExpire = 0;
    CWX_UINT32 ttNewExpire = 0;
    CWX_UINT32 uiOldVersion = 0;
	CWX_UINT32 uiBufLen = UNISTOR_MAX_KV_SIZE;
	char* szBuf = tss->getBuf(uiBufLen);
	int ret = _getKey(key.m_szData, key.m_uiDataLen, szBuf, uiBufLen, tss->m_szStoreKey, UNISTOR_MAX_KEY_SIZE, bCache, tss->m_szBuf2K);
    if (-1 == ret) return -1;
    if (1 == ret) getKvVersion(szBuf, uiBufLen, ttOldExpire, uiOldVersion);
    ///���㳬ʱʱ��
    if (m_config->getCommon().m_bEnableExpire){
        if((1==ret)&&(ttOldExpire<=m_ttExpireClock)) ret = 0; ///��ʱ
        if (1 == ret){
            ttNewExpire = ttOldExpire;
        }else{
            ttNewExpire = getNewExpire(uiExpire);
        }
    }else{
        ttNewExpire = 0;
    }

    uiFieldNum = 0;
    if (uiSign > 1) uiSign = 0;
	if ((0 == ret/*��ֵ������*/) ||
		(0 == uiSign)/*�滻����key*/)
    {
        if (!uiVersion){
            if (0 == ret){
                uiVersion = UNISTOR_KEY_START_VERION; ///��ʼ�汾
            }else{
                uiVersion = uiOldVersion + 1;
            }
        }
		if (field){
			tss->m_pEngineWriter->beginPack();
			if (!tss->m_pEngineWriter->addKeyValue(field->m_szData, field->m_uiDataLen, data.m_szData, data.m_uiDataLen, data.m_bKeyValue)){
				strcpy(tss->m_szBuf2K, tss->m_pEngineWriter->getErrMsg());
				return -1;
			}
			tss->m_pEngineWriter->pack();
			if (tss->m_pEngineWriter->getMsgSize() > UNISTOR_MAX_DATA_SIZE - getKvDataSignLen()){
				CwxCommon::snprintf(tss->m_szBuf2K, 2047, "Key/value is too big, max is [%u], now[%u]", UNISTOR_MAX_DATA_SIZE - getKvDataSignLen(), tss->m_pEngineWriter->getMsgSize());
				return -1;
			}
			uiBufLen = tss->m_pEngineWriter->getMsgSize();
			memcpy(szBuf, tss->m_pEngineWriter->getMsg(), uiBufLen);
			bNewKeyValue = true;
            uiFieldNum = 1;
		}else{
			if (data.m_uiDataLen > UNISTOR_MAX_DATA_SIZE - getKvDataSignLen()){
				CwxCommon::snprintf(tss->m_szBuf2K, 2047, "Key/value is too big, max is [%u], now[%u]", UNISTOR_MAX_DATA_SIZE - getKvDataSignLen(), data.m_uiDataLen);
				return -1;
			}
			uiBufLen = data.m_uiDataLen;
			memcpy(szBuf, data.m_szData, uiBufLen);
			bNewKeyValue = data.m_bKeyValue;
            uiFieldNum = iDataFieldNum;
		}
	}else{//key���ڶ��Ҳ���ȫ���滻
        if (!isKvData(szBuf, uiBufLen)){
            strcpy(tss->m_szBuf2K, "Key is key/value.");
            return -1;
        }
        if (!field && !data.m_bKeyValue){
            strcpy(tss->m_szBuf2K, "The set content is key/value.");
            return -1;
        }
        if (!uiVersion) uiVersion = uiOldVersion + 1;
        //��ʱ��key�ľɡ���valueһ��Ϊkey/value�ṹ
        ret = mergeSetKeyField(tss->m_pEngineWriter,
            tss->m_pEngineReader,
            tss->m_pEngineItemReader,
            key.m_szData,
            field,
            szBuf,
            uiBufLen-getKvDataSignLen(),
            true,
            data.m_szData,
            data.m_uiDataLen,
            data.m_bKeyValue,
            uiFieldNum,
            tss->m_szBuf2K);
        if (1 != ret) return ret;
        if (tss->m_pEngineWriter->getMsgSize() > UNISTOR_MAX_DATA_SIZE - getKvDataSignLen()){
            CwxCommon::snprintf(tss->m_szBuf2K, 2047, "Key/value is too big, max is [%u], now[%u]", UNISTOR_MAX_DATA_SIZE - getKvDataSignLen(), tss->m_pEngineWriter->getMsgSize());
            return -1;
        }
        uiBufLen = tss->m_pEngineWriter->getMsgSize();
        memcpy(szBuf, tss->m_pEngineWriter->getMsg(), uiBufLen);
        bNewKeyValue = true;
	}
    if (0 != appendSetBinlog(*tss->m_pEngineWriter,
        *tss->m_pEngineItemWriter,
        getKeyGroup(key.m_szData, key.m_uiDataLen),
        key,
        field,
        extra,
        data,
        uiExpire,
        uiSign,
        uiVersion,
        bCache,
        tss->m_szBuf2K))
    {
        m_bValid = false;
        strcpy(m_szErrMsg, tss->m_szBuf2K);
        return -1;
    }
    m_ullStoreSid = getCurSid();
    setKvDataSign(szBuf, uiBufLen, ttNewExpire, uiVersion, bNewKeyValue);
	if (0 != _setKey(key.m_szData, key.m_uiDataLen, szBuf, uiBufLen, ttOldExpire, bCache, tss->m_szBuf2K)) return -1;
	return 1;
}

///set key��1���ɹ���-1������0�������ڣ���������һ��key��fieldʱ��
int UnistorStoreBdb::syncSetKey(UnistorTss* tss,
                                CwxKeyValueItem const& key,
                                CwxKeyValueItem const* field,
                                CwxKeyValueItem const* ,
                                CwxKeyValueItem const& data,
                                CWX_UINT32 ,
                                CWX_UINT32 uiVersion,
                                bool bCache,
                                CWX_UINT32 uiExpire,
                                CWX_UINT64 ullSid,
                                bool  bRestore)
{
    if (!m_bValid){
        strcpy(tss->m_szBuf2K, m_szErrMsg);
        return -1;
    }
    CWX_UINT32 uiFieldNum = 0;
    bool bNewKeyValue = false;

    CWX_UINT32 uiBufLen = UNISTOR_MAX_KV_SIZE;
    CWX_UINT32 ttOldExpire = 0;
    CWX_UINT32 ttNewExpire = 0;
    CWX_UINT32 uiKeyVersion;
    char* szBuf = tss->getBuf(uiBufLen);
    int ret = _getKey(key.m_szData, key.m_uiDataLen, szBuf, uiBufLen, tss->m_szStoreKey, UNISTOR_MAX_KEY_SIZE, bCache, tss->m_szBuf2K);
    if (-1 == ret) return -1;
    if (1 == ret) getKvVersion(szBuf, uiBufLen, ttOldExpire, uiKeyVersion);
    ///���㳬ʱʱ��
    if (m_config->getCommon().m_bEnableExpire){
        if((1==ret) && (ttOldExpire<=m_ttExpireClock)) ret = 0; ///��ʱ
        if (1 == ret){
            ttNewExpire = ttOldExpire;
        }else{
            ttNewExpire = getNewExpire(uiExpire);
        }
    }else{
        ttNewExpire = 0;
    }

    if ((0 == ret/*��ֵ������*/) ||
        (!isKvData(szBuf, uiBufLen))/*��ֵ����kv��ֱ���滻*/ ||
        (!field && !data.m_uiDataLen)/*��ֵ����key/value*/)
    {
        if (field){
            tss->m_pEngineWriter->beginPack();
            if (!tss->m_pEngineWriter->addKeyValue(field->m_szData, field->m_uiDataLen, data.m_szData, data.m_uiDataLen, data.m_bKeyValue)){
                strcpy(tss->m_szBuf2K, tss->m_pEngineWriter->getErrMsg());
                return -1;
            }
            tss->m_pEngineWriter->pack();
            if (tss->m_pEngineWriter->getMsgSize() > UNISTOR_MAX_DATA_SIZE - getKvDataSignLen()){
                CwxCommon::snprintf(tss->m_szBuf2K, 2047, "Key/value is too big, max is [%u], now[%u]", UNISTOR_MAX_DATA_SIZE - getKvDataSignLen(), tss->m_pEngineWriter->getMsgSize());
                return -1;
            }
            uiBufLen = tss->m_pEngineWriter->getMsgSize();
            memcpy(szBuf, tss->m_pEngineWriter->getMsg(), uiBufLen);
            bNewKeyValue = true;
        }else{
            if (data.m_uiDataLen > UNISTOR_MAX_DATA_SIZE - getKvDataSignLen()){
                CwxCommon::snprintf(tss->m_szBuf2K, 2047, "Key/value is too big, max is [%u], now[%u]", UNISTOR_MAX_DATA_SIZE - getKvDataSignLen(), data.m_uiDataLen);
                return -1;
            }
            uiBufLen = data.m_uiDataLen;
            memcpy(szBuf, data.m_szData, uiBufLen);
            bNewKeyValue = data.m_bKeyValue;
        }
    }else{//key���ڣ��������������ֵ�merge
        if (bRestore){
            if (uiKeyVersion >= uiVersion){
                m_ullStoreSid = ullSid;
                return 1; ///�Ѿ�����
            }
        }
        //��ʱ��key�ľɡ���valueһ��Ϊkey/value�ṹ
        ret = mergeSetKeyField(tss->m_pEngineWriter,
            tss->m_pEngineReader,
            tss->m_pEngineItemReader,
            key.m_szData,
            field,
            szBuf,
            uiBufLen-getKvDataSignLen(),
            true,
            data.m_szData,
            data.m_uiDataLen,
            data.m_bKeyValue,
            uiFieldNum,
            tss->m_szBuf2K);
        if (1 != ret) return ret;
        if (tss->m_pEngineWriter->getMsgSize() > UNISTOR_MAX_DATA_SIZE - getKvDataSignLen()){
            CwxCommon::snprintf(tss->m_szBuf2K, 2047, "Key/value is too big, max is [%u], now[%u]", UNISTOR_MAX_DATA_SIZE - getKvDataSignLen(), tss->m_pEngineWriter->getMsgSize());
            return -1;
        }
        uiBufLen = tss->m_pEngineWriter->getMsgSize();
        memcpy(szBuf, tss->m_pEngineWriter->getMsg(), uiBufLen);
        bNewKeyValue = true;
    }
    if (ullSid > m_ullStoreSid)  m_ullStoreSid = ullSid;
    setKvDataSign(szBuf, uiBufLen, ttNewExpire, uiVersion, bNewKeyValue);
    if (0 != _setKey(key.m_szData, key.m_uiDataLen, szBuf, uiBufLen, ttOldExpire, bCache, tss->m_szBuf2K)) return -1;
    return 1;

}

///update key��1���ɹ���0�������ڣ�-1��ʧ�ܣ�-2���汾����
int UnistorStoreBdb::updateKey(UnistorTss* tss,
                               CwxKeyValueItem const& key,
                               CwxKeyValueItem const* field,
                               CwxKeyValueItem const* extra,
                               CwxKeyValueItem const& data,
                               CWX_UINT32 uiSign,
                               CWX_UINT32& uiVersion,
                               CWX_UINT32& uiFieldNum,
                               CWX_UINT32 uiExpire)
{
    int iDataFieldNum=0;
    if (!m_bValid){
        strcpy(tss->m_szBuf2K, m_szErrMsg);
        return -1;
    }
    if (data.m_bKeyValue){
        if (-1 == (iDataFieldNum = CwxPackage::getKeyValueNum(data.m_szData, data.m_uiDataLen))){
            strcpy(tss->m_szBuf2K, "The data is not valid key/value structure.");
            return -1;
        }
    }
    bool bNewKeyValue=false;
    CWX_UINT32 ttOldExpire = 0;
    CWX_UINT32 ttNewExpire = 0;
    CWX_UINT32 uiKeyVersion=0;
    CWX_UINT32 uiBufLen = UNISTOR_MAX_KV_SIZE;
    char* szBuf = tss->getBuf(uiBufLen);
    int ret = _getKey(key.m_szData, key.m_uiDataLen, szBuf, uiBufLen, tss->m_szStoreKey, UNISTOR_MAX_KEY_SIZE, true, tss->m_szBuf2K);
    if (-1 == ret) return -1;
    bool bOldKv = false;

    if (1 == ret) getKvVersion(szBuf, uiBufLen, ttOldExpire, uiKeyVersion);
    ///���㳬ʱʱ��
    if (m_config->getCommon().m_bEnableExpire){
        if((1==ret) && (ttOldExpire<=m_ttExpireClock)) ret = 0; ///��ʱ
        if (1 == ret){
            ttNewExpire = ttOldExpire;
        }else{
            ttNewExpire = getNewExpire(uiExpire);
        }
    }else{
        ttNewExpire = 0;
    }

    if (0 == ret){//not exist
        strcpy(tss->m_szBuf2K,"Key doesn't exist.");
        return 0; ///���ܵ�������field
    }
    //key����
    if (uiVersion){
        if (uiKeyVersion != uiVersion){
            CwxCommon::snprintf(tss->m_szBuf2K, 2047, "Key's version[%u] is not same with input version[%u].",
                uiKeyVersion, uiVersion);
            return -2;
        }
    }
    uiVersion = uiKeyVersion + 1;
    if (uiSign>2) uiSign = 1;
    bOldKv = isKvData(szBuf, uiBufLen);
    if (0 == uiSign){///��������key
        if (field){
            tss->m_pEngineWriter->beginPack();
            if (!tss->m_pEngineWriter->addKeyValue(field->m_szData,  field->m_uiDataLen, data.m_szData, data.m_uiDataLen, data.m_bKeyValue)){
                strcpy(tss->m_szBuf2K, tss->m_pEngineWriter->getErrMsg());
                return -1;
            }
            tss->m_pEngineWriter->pack();
            if (tss->m_pEngineWriter->getMsgSize() > UNISTOR_MAX_DATA_SIZE - getKvDataSignLen()){
                CwxCommon::snprintf(tss->m_szBuf2K, 2047, "Key/value is too big, max is [%u], now[%u]", UNISTOR_MAX_DATA_SIZE - getKvDataSignLen(), tss->m_pEngineWriter->getMsgSize());
                return -1;
            }
            uiBufLen = tss->m_pEngineWriter->getMsgSize();
            memcpy(szBuf, tss->m_pEngineWriter->getMsg(), uiBufLen);
            bNewKeyValue = true;
            uiFieldNum = 1;
        }else{
            if (data.m_uiDataLen > UNISTOR_MAX_DATA_SIZE - getKvDataSignLen()){
                CwxCommon::snprintf(tss->m_szBuf2K, 2047, "Key/value is too big, max is [%u], now[%u]", UNISTOR_MAX_DATA_SIZE - getKvDataSignLen(), data.m_uiDataLen);
                return -1;
            }
            uiBufLen = data.m_uiDataLen;
            memcpy(szBuf, data.m_szData, data.m_uiDataLen);
            bNewKeyValue = data.m_bKeyValue;
            uiFieldNum = iDataFieldNum;
        }
    }else{//��ʱ���¡���ֵȫ��Ϊkv
        if (!bOldKv){
            strcpy(tss->m_szBuf2K, "The old data is not key/value");
            return -1;
        }
        if (!field && !data.m_bKeyValue){
            strcpy(tss->m_szBuf2K, "The new data is not key/value");
        }
        ret = mergeUpdateKeyField(tss->m_pEngineWriter,
            tss->m_pEngineReader,
            tss->m_pEngineItemReader,
            key.m_szData,
            field,
            szBuf,
            uiBufLen-getKvDataSignLen(),
            bOldKv,
            data.m_szData,
            data.m_uiDataLen,
            data.m_bKeyValue,
            uiFieldNum,
            (2==uiSign)?true:false,
            tss->m_szBuf2K);
        if (1 != ret) return ret;
        if (tss->m_pEngineWriter->getMsgSize() > UNISTOR_MAX_DATA_SIZE - getKvDataSignLen()){
            CwxCommon::snprintf(tss->m_szBuf2K, 2047, "Key/value is too big, max is [%u], now[%u]", UNISTOR_MAX_DATA_SIZE - getKvDataSignLen(), tss->m_pEngineWriter->getMsgSize());
            return -1;
        }
        uiBufLen = tss->m_pEngineWriter->getMsgSize();
        memcpy(szBuf, tss->m_pEngineWriter->getMsg(), uiBufLen);
        bNewKeyValue = true;
    }

    if (0 != appendUpdateBinlog(*tss->m_pEngineWriter,
        *tss->m_pEngineItemWriter,
        getKeyGroup(key.m_szData, key.m_uiDataLen),
        key,
        field,
        extra,
        data,
        uiExpire,
        uiSign,
        uiVersion,
        tss->m_szBuf2K))
    {
        m_bValid = false;
        strcpy(m_szErrMsg, tss->m_szBuf2K);
        return -1;
    }
    m_ullStoreSid = getCurSid();
    setKvDataSign(szBuf, uiBufLen, ttNewExpire, uiVersion, bNewKeyValue);
    if (0 != _setKey(key.m_szData, key.m_uiDataLen, szBuf, uiBufLen, ttOldExpire, true, tss->m_szBuf2K)) return -1;
    return 1;
}

///update key��1���ɹ���0�������ڣ�-1��ʧ��
int UnistorStoreBdb::syncUpdateKey(UnistorTss* tss,
                                   CwxKeyValueItem const& key,
                                   CwxKeyValueItem const* field,
                                   CwxKeyValueItem const* extra,
                                   CwxKeyValueItem const& data,
                                   CWX_UINT32 uiSign,
                                   CWX_UINT32 uiVersion,
                                   CWX_UINT32 uiExpire,
                                   CWX_UINT64 ullSid,
                                   bool  bRestore)
{
    return syncSetKey(tss,
        key,
        field,
        extra,
        data,
        uiSign,
        uiVersion,
        true,
        uiExpire,
        ullSid,
        bRestore);
}


///inc key��1���ɹ���0�������ڣ�-1��ʧ�ܣ�-2:�汾����-3�������߽�
int UnistorStoreBdb::incKey(UnistorTss* tss,
                            CwxKeyValueItem const& key,
                            CwxKeyValueItem const* field,
                            CwxKeyValueItem const* extra,
                            CWX_INT32 num,
                            CWX_INT64  llMax,
                            CWX_INT64  llMin,
                            CWX_UINT32  uiSign,
                            CWX_INT64& llValue,
                            CWX_UINT32& uiVersion,
                            CWX_UINT32 uiExpire)
{
    if (!m_bValid){
        strcpy(tss->m_szBuf2K, m_szErrMsg);
        return -1;
    }
    bool bNewKeyValue=false;
    CWX_UINT32 ttOldExpire = 0;
    CWX_UINT32 ttNewExpire = 0;
    CWX_UINT32 uiKeyVersion = 0;
    CWX_UINT32 uiBufLen = UNISTOR_MAX_KV_SIZE;
    char* szBuf = tss->getBuf(uiBufLen);
    int ret = _getKey(key.m_szData, key.m_uiDataLen, szBuf, uiBufLen, tss->m_szStoreKey, UNISTOR_MAX_KEY_SIZE, true, tss->m_szBuf2K);
    if (-1 == ret) return -1;
    bool bOldKv = false;
    CWX_UINT32 uiOutBufLen = UNISTOR_MAX_KV_SIZE;
    bool bKeyValue = false;
    if (1 == ret) getKvVersion(szBuf, uiBufLen, ttOldExpire, uiKeyVersion);
    ///���㳬ʱʱ��
    if (m_config->getCommon().m_bEnableExpire){
        if((1==ret) && (ttOldExpire<=m_ttExpireClock)) ret = 0; ///��ʱ
        if (1 == ret){
            ttNewExpire = ttOldExpire;
        }else{
            ttNewExpire = getNewExpire(uiExpire);
        }
    }else{
        ttNewExpire = 0;
    }

    if (1 == ret){
        if (uiVersion && (uiKeyVersion != uiVersion)){
            CwxCommon::snprintf(tss->m_szBuf2K, 2047, "Key's version[%s] is not same with input version[%s].",
                uiKeyVersion,
                uiVersion);
            return -2; ///�汾����
        }
        uiVersion ++;
    }else{
        uiVersion = UNISTOR_KEY_START_VERION; 
    }
    if (uiSign > 2) uiSign = 0;

    if (0 == ret){//not exist
        if (2 != uiSign){
            CwxCommon::snprintf(tss->m_szBuf2K, 2047, "Key[%s] doesn't exist.", key.m_szData);
            return 0; ///���ܵ�������field
        }
        if (field){
            tss->m_pEngineWriter->beginPack();
            if (!tss->m_pEngineWriter->addKeyValue(field->m_szData, field->m_uiDataLen, num)){
                strcpy(tss->m_szBuf2K, tss->m_pEngineWriter->getErrMsg());
                return -1;
            }
            tss->m_pEngineWriter->pack();
            if (tss->m_pEngineWriter->getMsgSize() > UNISTOR_MAX_DATA_SIZE - getKvDataSignLen()){
                CwxCommon::snprintf(tss->m_szBuf2K, 2047, "Key/value is too big, max is [%u], now[%u]", UNISTOR_MAX_DATA_SIZE - getKvDataSignLen(), tss->m_pEngineWriter->getMsgSize());
                return -1;
            }
            uiBufLen = tss->m_pEngineWriter->getMsgSize();
            memcpy(szBuf, tss->m_pEngineWriter->getMsg(), uiBufLen);
            bNewKeyValue = true;
        }else{
            CwxCommon::toString((CWX_INT64)num, szBuf, 0);
            uiBufLen = strlen(szBuf);
            bNewKeyValue=false;
        }
    }else{
        bOldKv = isKvData(szBuf, uiBufLen);
        ret = mergeIncKeyField(tss->m_pEngineWriter,
            tss->m_pEngineReader,
            key.m_szData,
            field,
            szBuf,
            uiBufLen - getKvDataSignLen(),
            bOldKv,
            num,
            llMax,
            llMin,
            llValue,
            szBuf,
            uiOutBufLen,
            bKeyValue,
            uiSign,
            tss->m_szBuf2K);
        if (1 != ret){
            if (-2 == ret) ret = -3;
            return ret;
        }
        if (uiOutBufLen > UNISTOR_MAX_DATA_SIZE - getKvDataSignLen()){
            CwxCommon::snprintf(tss->m_szBuf2K, 2047, "Key/value is too big, max is [%u], now[%u]", UNISTOR_MAX_DATA_SIZE - getKvDataSignLen(), uiOutBufLen);
            return -1;
        }
        uiBufLen = uiOutBufLen;
        bNewKeyValue=bKeyValue;
    }

    if (0 != appendIncBinlog(*tss->m_pEngineWriter,
        *tss->m_pEngineItemWriter,
        getKeyGroup(key.m_szData, key.m_uiDataLen),
        key,
        field,
        extra,
        num,
        llMax,
        llMin,
        uiExpire,
        uiSign,
        uiVersion,
        tss->m_szBuf2K))
    {
        m_bValid = false;
        strcpy(m_szErrMsg, tss->m_szBuf2K);
        return -1;
    }
    m_ullStoreSid = getCurSid();

    setKvDataSign(szBuf, uiBufLen, ttNewExpire, uiVersion, bNewKeyValue);
    if (0 != _setKey(key.m_szData, key.m_uiDataLen, szBuf, uiBufLen, ttOldExpire, true, tss->m_szBuf2K))  return -1;
    return 1;
}



///inc key��1���ɹ���0�������ڣ�-1��ʧ�ܣ�
int UnistorStoreBdb::syncIncKey(UnistorTss* tss,
                                CwxKeyValueItem const& key,
                                CwxKeyValueItem const* field,
                                CwxKeyValueItem const* ,
                                CWX_INT32 num,
                                CWX_INT64  llMax,
                                CWX_INT64  llMin,
                                CWX_UINT32 ,
                                CWX_INT64& llValue,
                                CWX_UINT32 uiVersion,
                                CWX_UINT32 uiExpire,
                                CWX_UINT64 ullSid,
                                bool  bRestore)
{
    if (!m_bValid){
        strcpy(tss->m_szBuf2K, m_szErrMsg);
        return -1;
    }
    bool bNewKeyValue=false;
    CWX_UINT32 ttOldExpire = 0;
    CWX_UINT32 ttNewExpire = 0;
    CWX_UINT32 uiKeyVersion = 0;
    CWX_UINT32 uiBufLen = UNISTOR_MAX_KV_SIZE;
    char* szBuf = tss->getBuf(uiBufLen);
    int ret = _getKey(key.m_szData, key.m_uiDataLen, szBuf, uiBufLen, tss->m_szStoreKey, UNISTOR_MAX_KEY_SIZE, true, tss->m_szBuf2K);
    if (-1 == ret) return -1;
    bool bOldKv = false;
    CWX_UINT32 uiOutBufLen = UNISTOR_MAX_KV_SIZE;
    bool bKeyValue = false;
    if (1 == ret) getKvVersion(szBuf, uiBufLen, ttOldExpire, uiKeyVersion);
    ///���㳬ʱʱ��
    if (m_config->getCommon().m_bEnableExpire){
        if((1==ret) && (ttOldExpire<=m_ttExpireClock)) ret = 0; ///��ʱ
        if (1 == ret){
            ttNewExpire = ttOldExpire;
        }else{
            ttNewExpire = getNewExpire(uiExpire);
        }
    }else{
        ttNewExpire = 0;
    }

    if (0 == ret){//not exist
        if (field){
            tss->m_pEngineWriter->beginPack();
            tss->m_pEngineWriter->addKeyValue(field->m_szData, field->m_uiDataLen, num);
            tss->m_pEngineWriter->pack();
            memcpy(szBuf, tss->m_pEngineWriter->getMsg(), tss->m_pEngineWriter->getMsgSize());
            uiBufLen = tss->m_pEngineWriter->getMsgSize();
            bNewKeyValue = true;
        }else{
            CwxCommon::toString((CWX_INT64)num, szBuf, 0);
            uiBufLen = strlen(szBuf);
            bNewKeyValue=false;
        }
    }else{
        if (bRestore){
            if (uiKeyVersion >= uiVersion){
                m_ullStoreSid = ullSid;
                return 1;
            }
        }
        bOldKv = isKvData(szBuf, uiBufLen);
        ret = mergeIncKeyField(tss->m_pEngineWriter,
            tss->m_pEngineReader,
            key.m_szData,
            field,
            szBuf,
            uiBufLen - getKvDataSignLen(),
            bOldKv,
            num,
            llMax,
            llMin,
            llValue,
            szBuf,
            uiOutBufLen,
            bKeyValue,
            2,
            tss->m_szBuf2K);
        if (1 != ret){
            if (-2 == ret) ret = -3;
            return ret;
        }
        if (uiOutBufLen > UNISTOR_MAX_DATA_SIZE - getKvDataSignLen()){
            CwxCommon::snprintf(tss->m_szBuf2K, 2047, "Key/value is too big, max is [%u], now[%u]", UNISTOR_MAX_DATA_SIZE - getKvDataSignLen(), uiOutBufLen);
            return -1;
        }
        uiBufLen = uiOutBufLen;
        bNewKeyValue=bKeyValue;
    }
    if (ullSid > m_ullStoreSid)  m_ullStoreSid = ullSid;
    setKvDataSign(szBuf, uiBufLen, ttNewExpire, uiVersion, bNewKeyValue);
    if (0 != _setKey(key.m_szData, key.m_uiDataLen, szBuf, uiBufLen, ttOldExpire, true, tss->m_szBuf2K)) return -1;
    return 1;
}

///inc key��1���ɹ���0�������ڣ�-1��ʧ�ܣ�-2:�汾����
int UnistorStoreBdb::delKey(UnistorTss* tss,
                            CwxKeyValueItem const& key,
                            CwxKeyValueItem const* field,
                            CwxKeyValueItem const* extra,
                            CWX_UINT32& uiVersion,
                            CWX_UINT32& uiFieldNum)
{
    uiFieldNum = 0;
    if (!m_bValid){
        strcpy(tss->m_szBuf2K, m_szErrMsg);
        return -1;
    }
    bool bNewKeyValue=false;
    CWX_UINT32 ttOldExpire = 0;
    CWX_UINT32 uiKeyVersion = 0;

    CWX_UINT32 uiBufLen = UNISTOR_MAX_KV_SIZE;
    char* szBuf = tss->getBuf(uiBufLen);
    int ret = _getKey(key.m_szData, key.m_uiDataLen, szBuf, uiBufLen, tss->m_szStoreKey, UNISTOR_MAX_KEY_SIZE, true,  tss->m_szBuf2K);
    if (-1 == ret) return -1;
    bool bOldKv = false;
    if (0 == ret){//not exist
        CwxCommon::snprintf(tss->m_szBuf2K, 2047, "Key[%s] doesn't exist.", key.m_szData);
        return 0;
    }else{
        getKvVersion(szBuf, uiBufLen, ttOldExpire, uiKeyVersion);
        if (uiVersion){
            if (uiVersion != uiKeyVersion){
                CwxCommon::snprintf(tss->m_szBuf2K, 2047, "Key's version[%s] is not same with input version[%s].",
                    uiKeyVersion,
                    uiVersion);
                return -2;
            }
        }
        uiVersion = uiKeyVersion + 1;
        bOldKv = isKvData(szBuf, uiBufLen);
        if (field){//ɾ��һ��field
            if (!bOldKv){
                CwxCommon::snprintf(tss->m_szBuf2K, 2047, "Key[%s] isn't key/value, can't delete field.", key.m_szData, field->m_szData);
                return 0;
            }
            ret = mergeRemoveKeyField(tss->m_pEngineWriter,
                tss->m_pEngineReader,
                key.m_szData,
                field,
                szBuf,
                uiBufLen-getKvDataSignLen(),
                bOldKv,
                false,
                uiFieldNum,
                tss->m_szBuf2K);
            if (1 != ret) return ret;
            if (tss->m_pEngineWriter->getMsgSize() > UNISTOR_MAX_DATA_SIZE - getKvDataSignLen()){
                CwxCommon::snprintf(tss->m_szBuf2K, 2047, "Key/value is too big, max is [%u], now[%u]", UNISTOR_MAX_DATA_SIZE - getKvDataSignLen(), tss->m_pEngineWriter->getMsgSize());
                return -1;
            }
            uiBufLen = tss->m_pEngineWriter->getMsgSize();
            memcpy(szBuf, tss->m_pEngineWriter->getMsg(), uiBufLen);
            bNewKeyValue = true;
        }
    }
    if (0 != appendDelBinlog(*tss->m_pEngineWriter,
        *tss->m_pEngineItemWriter,
        getKeyGroup(key.m_szData, key.m_uiDataLen),
        key,
        field,
        extra,
        uiVersion,
        tss->m_szBuf2K))
    {
        m_bValid = false;
        strcpy(m_szErrMsg, tss->m_szBuf2K);
        return -1;
    }
    m_ullStoreSid = getCurSid();
    if (!field){
        uiFieldNum = 0;
        if (0 != _delKey(key.m_szData, key.m_uiDataLen, ttOldExpire, tss->m_szBuf2K)) return -1;
    }else{
        setKvDataSign(szBuf, uiBufLen, ttOldExpire, uiVersion, bNewKeyValue);
        if (0 != _setKey(key.m_szData, key.m_uiDataLen, szBuf, uiBufLen, ttOldExpire, true, tss->m_szBuf2K)) return -1;
    }
    return 1;
}
///inc key��1���ɹ���0�������ڣ�-1��ʧ�ܣ�
int UnistorStoreBdb::syncDelKey(UnistorTss* tss,
                                CwxKeyValueItem const& key,
                                CwxKeyValueItem const* field,
                                CwxKeyValueItem const* ,
                                CWX_UINT32 uiVersion,
                                CWX_UINT64 ullSid,
                                bool  bRestore)
{
    if (!m_bValid){
        strcpy(tss->m_szBuf2K, m_szErrMsg);
        return -1;
    }
    CWX_UINT32 uiFieldNum = 0;
    bool bNewKeyValue=false;
    CWX_UINT32 ttOldExpire = 0;
    CWX_UINT32 uiKeyVersion = 0;

    CWX_UINT32 uiBufLen = UNISTOR_MAX_KV_SIZE;
    char* szBuf = tss->getBuf(uiBufLen);
    int ret = _getKey(key.m_szData, key.m_uiDataLen, szBuf, uiBufLen, tss->m_szStoreKey, UNISTOR_MAX_KEY_SIZE, true,  tss->m_szBuf2K);
    if (-1 == ret) return -1;
    bool bOldKv = false;
    if (0 == ret){//not exist
        CwxCommon::snprintf(tss->m_szBuf2K, 2047, "Key[%s] doesn't exist.", key.m_szData);
        return 0;
    }else{
        getKvVersion(szBuf, uiBufLen, ttOldExpire, uiKeyVersion);
        if (bRestore){
            if (uiKeyVersion >= uiVersion){
                m_ullStoreSid = ullSid;
                return 1;
            }
        }
        bOldKv = isKvData(szBuf, uiBufLen);
        if (field){//ɾ��һ��field
            if (!bOldKv){
                return 1;
            }
            ret = mergeRemoveKeyField(tss->m_pEngineWriter,
                tss->m_pEngineReader,
                key.m_szData,
                field,
                szBuf,
                uiBufLen-getKvDataSignLen(),
                bOldKv,
                true,
                uiFieldNum,
                tss->m_szBuf2K);
            if (1 != ret) return ret;
            if (tss->m_pEngineWriter->getMsgSize() > UNISTOR_MAX_DATA_SIZE - getKvDataSignLen()){
                CwxCommon::snprintf(tss->m_szBuf2K, 2047, "Key/value is too big, max is [%u], now[%u]", UNISTOR_MAX_DATA_SIZE - getKvDataSignLen(), tss->m_pEngineWriter->getMsgSize());
                return -1;
            }
            uiBufLen = tss->m_pEngineWriter->getMsgSize();
            memcpy(szBuf, tss->m_pEngineWriter->getMsg(), uiBufLen);
            bNewKeyValue = true;
        }
        //����ɾ������key
    }
    if (ullSid > m_ullStoreSid)  m_ullStoreSid = ullSid;
    if (!field){
        if (0 != _delKey(key.m_szData, key.m_uiDataLen, ttOldExpire, tss->m_szBuf2K)) return -1;
    }else{
        setKvDataSign(szBuf, uiBufLen, ttOldExpire, uiVersion, bNewKeyValue);
        if (0 != _setKey(key.m_szData, key.m_uiDataLen, szBuf, uiBufLen, ttOldExpire, true, tss->m_szBuf2K)) return -1;
    }
    return 1;
}

///��ȡkey, 1���ɹ���0�������ڣ�-1��ʧ��;
int UnistorStoreBdb::get(UnistorTss* tss,
                         CwxKeyValueItem const& key,
                         CwxKeyValueItem const* field,
                         CwxKeyValueItem const* ,
                         char const*& szData,
                         CWX_UINT32& uiLen,
                         bool& bKeyValue,
                         CWX_UINT32& uiVersion,
                         CWX_UINT32& uiFieldNum,
                         bool bKeyInfo)
{
    uiLen = UNISTOR_MAX_KV_SIZE;
    int ret = 0;
    uiFieldNum = 0;
    szData = tss->getBuf(uiLen);
	ret = _getKey(key.m_szData, key.m_uiDataLen, (char*)szData, uiLen, tss->m_szStoreKey, UNISTOR_MAX_KEY_SIZE, true, tss->m_szBuf2K);
	if (1 == ret){//key����
        CWX_UINT32 ttOldExpire = 0;
        getKvVersion(szData, uiLen, ttOldExpire, uiVersion);
        if (m_config->getCommon().m_bEnableExpire && (ttOldExpire<=m_ttExpireClock)) return 0;
		bKeyValue = isKvData(szData, uiLen);
		if (uiLen) uiLen -= getKvDataSignLen();
        if (bKeyValue){
            uiFieldNum = CwxPackage::getKeyValueNum(szData, uiLen);
        }
        UnistorKeyField* fieldKey = NULL;
        if (bKeyInfo){
            uiLen = sprintf((char*)szData,"%u,%u,%u,%u", ttOldExpire, uiVersion, uiLen, bKeyValue?1:0);
            bKeyValue = false;
        }else if (field){
            if (!bKeyValue){
                CwxCommon::snprintf(tss->m_szBuf2K, 2047, "Key[%s]'s data isn't key/value.", key.m_szData);
                return -1;
            }
            UnistorStoreBase::parseMultiField(field->m_szData, fieldKey);
            if (UNISTOR_ERR_SUCCESS != UnistorStoreBase::pickField(*tss->m_pEngineReader, *tss->m_pEngineWriter, fieldKey, szData, uiLen, tss->m_szBuf2K)){
                UnistorStoreBase::freeField(fieldKey);
                return -1;
            }
            uiLen = tss->m_pEngineWriter->getMsgSize();
            memcpy((char*)szData, tss->m_pEngineWriter->getMsg(), uiLen);
        }
        if (fieldKey) UnistorStoreBase::freeField(fieldKey);
        return 1;
    }else if (0 == ret){
        return 0;
	}
	return -1;
}

///��ȡkey, 1���ɹ���0�������ڣ�-1��ʧ��;
int UnistorStoreBdb::gets(UnistorTss* tss,
                 list<pair<char const*, CWX_UINT16> > const& keys,
                 CwxKeyValueItem const* field,
                 CwxKeyValueItem const* ,
                 char const*& szData,
                 CWX_UINT32& uiLen,
                 bool bKeyInfo)
{
    int ret = 0;
    CWX_UINT32 uiVersion = 0;
    CWX_UINT32 ttOldExpire = 0;
    bool bKeyValue = false;
    UnistorKeyField* fieldKey = NULL;
    list<pair<char const*, CWX_UINT16> >::const_iterator iter = keys.begin();
    szData = tss->getBuf(UNISTOR_MAX_KV_SIZE);
    if (field) UnistorStoreBase::parseMultiField(field->m_szData, fieldKey);
    tss->m_pEngineWriter->beginPack();
    while(iter != keys.end()){
        if (tss->m_pEngineWriter->getMsgSize() > UNISTOR_MAX_KVS_SIZE){
            if (fieldKey) UnistorStoreBase::freeField(fieldKey);
            CwxCommon::snprintf(tss->m_szBuf2K, 2048, "Output's data size is too big[%u], max is %u", tss->m_pEngineWriter->getMsgSize(), UNISTOR_MAX_KVS_SIZE);
            return -1;
        }
        do{
            uiLen = UNISTOR_MAX_KV_SIZE;
            ret = _getKey(iter->first, iter->second, (char*)szData, uiLen, tss->m_szStoreKey, UNISTOR_MAX_KEY_SIZE, true, tss->m_szBuf2K);
            if (1 == ret){//key����
                getKvVersion(szData, uiLen, ttOldExpire, uiVersion);
                if (m_config->getCommon().m_bEnableExpire && (ttOldExpire<=m_ttExpireClock)){///timeout
                    break;
                }else{
                    bKeyValue = isKvData(szData, uiLen);
                    if (uiLen) uiLen -= getKvDataSignLen();
                }
                if (bKeyInfo){
                    uiLen = sprintf((char*)szData,"%u,%u,%u,%u", ttOldExpire, uiVersion, uiLen, bKeyValue?1:0);
                    tss->m_pEngineWriter->addKeyValue(iter->first,iter->second, szData, uiLen, false);
                }else if (fieldKey){
                    if (!bKeyValue ||
                        (UNISTOR_ERR_SUCCESS != UnistorStoreBase::pickField(*tss->m_pEngineReader, *tss->m_pEngineItemWriter, fieldKey, szData, uiLen, tss->m_szBuf2K)))
                    {
                        tss->m_pEngineWriter->addKeyValue(iter->first, iter->second, "", 0, true);
                    }else{
                        tss->m_pEngineWriter->addKeyValue(iter->first, iter->second, tss->m_pEngineItemWriter->getMsg(), tss->m_pEngineItemWriter->getMsgSize(), true);
                    }
                }else{
                    tss->m_pEngineWriter->addKeyValue(iter->first, iter->second, szData, uiLen, bKeyValue);
                }
            }
        }while(0);
        iter++;
    }
    tss->m_pEngineWriter->pack();
    if (fieldKey) UnistorStoreBase::freeField(fieldKey);
    uiLen = tss->m_pEngineWriter->getMsgSize();
    szData = tss->getBuf(uiLen);
    memcpy((char*)szData, tss->m_pEngineWriter->getMsg(), uiLen);
    return 1;
}


///�����αꡣ-1���ڲ�����ʧ�ܣ�0����֧�֣�1���ɹ�
int UnistorStoreBdb::createCursor(UnistorStoreCursor& cursor,
                                  CwxKeyValueItem const* field,
                                  CwxKeyValueItem const*,
                                  char* szErr2K)
{
    if (!m_bdb){
        if (szErr2K) strcpy(szErr2K, m_szErrMsg);
        return -1;
    }
    UnistorStoreBdbCursor* pCursor =  new UnistorStoreBdbCursor();
    cursor.m_field = NULL;
    if (field){
        UnistorStoreBase::parseMultiField(field->m_szData, cursor.m_field);
    }
    int ret = m_bdb->cursor(m_bdb, 0, &pCursor->m_cursor, DB_READ_UNCOMMITTED);
    if (0 != ret){
        if (cursor.m_field){
            UnistorStoreBase::freeField(cursor.m_field);
            cursor.m_field = NULL;
        }
        delete pCursor;
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "Failure to create cursor. err-code:%d, err-msg=%s",
            ret,
            db_strerror(ret));
        return -1;
    }
    pCursor->m_bFirst = true;
    cursor.m_cursorHandle = pCursor;
    return 1;
}

///��ȡ���ݡ�-1��ʧ�ܣ�0��������1����ȡһ��
int UnistorStoreBdb::next(UnistorTss* tss,
                          UnistorStoreCursor& cursor,
                          char const*& szKey,
                          CWX_UINT16& unKeyLen,
                          char const*& szData,
                          CWX_UINT32& uiDataLen,
                          bool& bKeyValue,
                          CWX_UINT32& uiVersion,
                          bool bKeyInfo)
{
    int ret = 0;
    bool bUserCache = false;
    CWX_UINT32 ttExpire=0;
    uiDataLen = UNISTOR_MAX_KV_SIZE;
    szData = tss->getBuf(uiDataLen);
    do{
        if (cursor.m_bStoreMore && !cursor.m_bStoreValue){///��ȡstore��ֵ
            cursor.m_unStoreKeyLen = UNISTOR_MAX_KEY_SIZE;
            cursor.m_uiStoreDataLen = UNISTOR_MAX_KV_SIZE;
            ret = _nextBdb(cursor, tss->m_szBuf2K);
            if (-1 == ret) return -1;
            if (0 == ret){
                cursor.m_bStoreMore = false;
            }else{
                cursor.m_bStoreValue = true;
            }
        }
        if (cursor.m_bCacheMore && !cursor.m_bCacheValue){///��ȡcache��ֵ
            ret = nextCache(cursor, tss->m_szBuf2K);
            if (-1 == ret) return -1;
            if (0 == ret){
                cursor.m_bCacheMore = false;
            }else{
                cursor.m_bCacheValue = true;
            }
        }
        if (!cursor.m_bStoreMore && !cursor.m_bCacheMore) return 0; ///û������

        bUserCache = false;
        if (cursor.m_bCacheMore){
            if (!cursor.m_bStoreMore){
                bUserCache = true;
            }else{
                if (cursor.m_asc){
                    ret = keyAsciiCmpLess(cursor.m_szCacheKey,
                        strlen(cursor.m_szCacheKey),
                        cursor.m_szStoreKey,
                        strlen(cursor.m_szStoreKey));
                    if (ret < 0){
                        bUserCache = true;
                    }else if (0 == ret){
                        bUserCache = true;
                        cursor.m_bStoreValue = false; ///store�е�ֵ���ϣ���Ϊ�ظ�
                    }
                }else{
                    ret = keyAsciiCmpLess(cursor.m_szCacheKey,
                        strlen(cursor.m_szCacheKey),
                        cursor.m_szStoreKey,
                        strlen(cursor.m_szStoreKey));
                    if (ret > 0){
                        bUserCache = true;
                    }else if (0 == ret){
                        bUserCache = true;
                        cursor.m_bStoreValue = false;///store�е�ֵ���ϣ���Ϊ�ظ�
                    }
                }
            }
        }
        ///����Ƿ�ʱ
        if (bUserCache){
            if (cursor.m_bCacheDel){
                cursor.m_bCacheValue = false;
                continue; ///��������
            }
            getKvVersion(cursor.m_szCacheData, cursor.m_uiCacheDataLen, ttExpire, uiVersion);
            if (m_config->getCommon().m_bEnableExpire && (ttExpire<=m_ttExpireClock)){
                cursor.m_bCacheValue = false;
                continue;
            }
        }else{
            getKvVersion(cursor.m_szStoreData, cursor.m_uiStoreDataLen, ttExpire, uiVersion);
            if (m_config->getCommon().m_bEnableExpire && (ttExpire<=m_ttExpireClock)){
                cursor.m_bStoreValue = false;
                continue;
            }
        }
        break;
    }while(1);
    if (bUserCache){
        cursor.m_bCacheValue = false;
        szKey = cursor.m_szCacheKey;
        unKeyLen = cursor.m_unCacheKeyLen;
        bKeyValue = isKvData(cursor.m_szCacheData, cursor.m_uiCacheDataLen);
        uiDataLen = cursor.m_uiCacheDataLen - getKvDataSignLen();
        if (bKeyInfo){
            uiDataLen = sprintf((char*)szData, "%u,%u,%u,%u", ttExpire, uiVersion, uiDataLen, bKeyValue?1:0);
            bKeyValue = false;
        }else if (!cursor.m_field){
            szData = cursor.m_szCacheData;
        }else if(!bKeyValue){
            szData="";
            uiDataLen = 0;
            bKeyValue = false;
        }else{
            ret = UnistorStoreBase::pickField(*tss->m_pEngineReader,
                *tss->m_pEngineWriter,
                cursor.m_field,
                cursor.m_szCacheData,
                uiDataLen,
                tss->m_szBuf2K);
            if (ret == UNISTOR_ERR_SUCCESS){
                uiDataLen = tss->m_pEngineWriter->getMsgSize();
                szData = tss->m_pEngineWriter->getMsg();
                bKeyValue = true;
            }else{
                szData="";
                uiDataLen = 0;
                bKeyValue = true;
            }
        }
    }else{
        cursor.m_bStoreValue = false;
        szKey=cursor.m_szStoreKey;
        unKeyLen = cursor.m_unStoreKeyLen;
        bKeyValue = isKvData(cursor.m_szStoreData, cursor.m_uiStoreDataLen);
        uiDataLen =cursor.m_uiStoreDataLen-getKvDataSignLen();
        if (bKeyInfo){
            uiDataLen = sprintf((char*)szData,"%u,%u,%u,%u", ttExpire, uiVersion, uiDataLen, bKeyValue?1:0);
            bKeyValue = false;
        }else if (!cursor.m_field){
            szData=cursor.m_szStoreData;
        }else if(!bKeyValue){
            szData="";
            uiDataLen = 0;
            bKeyValue = true;
        }else{
            ret = UnistorStoreBase::pickField(*tss->m_pEngineReader,
                *tss->m_pEngineWriter,
                cursor.m_field,
                cursor.m_szStoreData,
                uiDataLen,
                tss->m_szBuf2K);
            if (ret == UNISTOR_ERR_SUCCESS){
                uiDataLen = tss->m_pEngineWriter->getMsgSize();
                szData = tss->m_pEngineWriter->getMsg();
                bKeyValue = true;
            }else{
                szData="";
                uiDataLen = 0;
                bKeyValue = true;
            }
        }
    }
    ((char*)szKey)[unKeyLen]=0x00;
    return 1;
}
///��ȡ���ݡ�-1��ʧ�ܣ�0��������1����ȡһ��
int UnistorStoreBdb::_nextBdb(UnistorStoreCursor& cursor, char* szErr2K){
    UnistorStoreBdbCursor* pCursor = (UnistorStoreBdbCursor*)cursor.m_cursorHandle;
    DBT bdb_key;
    DBT bdb_data;
    int ret = 0;
    do{
        memset(&bdb_key, 0x00, sizeof(bdb_key));
        memset(&bdb_data, 0x00, sizeof(bdb_data));

        bdb_key.data = (void*)cursor.m_szStoreKey;
        bdb_key.flags=DB_DBT_USERMEM;
        bdb_key.ulen = UNISTOR_MAX_KEY_SIZE;

        bdb_data.data = (void*)cursor.m_szStoreData;
        bdb_data.ulen = UNISTOR_MAX_KV_SIZE;
        bdb_data.flags = DB_DBT_USERMEM;

        do{
            if (pCursor->m_bFirst){///��һ��
                pCursor->m_bFirst = false;
                if (!cursor.m_begin){///û��ָ����Χ
                    if (cursor.m_asc){
                        if ((ret = pCursor->m_cursor->get(pCursor->m_cursor, &bdb_key, &bdb_data, DB_FIRST)) != 0){
                            if (DB_NOTFOUND == ret) return 0;
                            if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "Failure to move cursor to begin. err-code:%d, err-msg=%s",
                                ret,
                                db_strerror(ret));
                            return -1;
                        }
                    }else{
                        if ((ret = pCursor->m_cursor->get(pCursor->m_cursor, &bdb_key, &bdb_data, DB_LAST)) != 0){
                            if (DB_NOTFOUND == ret) return 0;
                            if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "Failure to move cursor to last. err-code:%d, err-msg=%s",
                                ret,
                                db_strerror(ret));
                            return -1;
                        }
                    }
                    break; ///����
                }
                bdb_key.size = strlen(cursor.m_begin);
                memcpy(cursor.m_szStoreKey, cursor.m_begin, bdb_key.size);
                if ((ret = pCursor->m_cursor->get(pCursor->m_cursor, &bdb_key, &bdb_data, DB_SET_RANGE)) != 0){
                    if (DB_NOTFOUND == ret){
                        if (cursor.m_asc) return 0;
                        if ((ret = pCursor->m_cursor->get(pCursor->m_cursor, &bdb_key, &bdb_data, DB_LAST)) != 0){
                            if (DB_NOTFOUND == ret) return 0;
                            if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "Failure to move cursor to last. err-code:%d, err-msg=%s",
                                ret,
                                db_strerror(ret));
                            return -1;
                        }
                    }else{
                        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "Failure to move cursor to key:%s. err-code:%d, err-msg=%s",
                            cursor.m_begin?cursor.m_begin:"",
                            ret,
                            db_strerror(ret));
                        return -1;
                    }
                }
                ///����Ƿ������ʼֵ
                ret = keyAsciiCmpLess(cursor.m_begin,
                    cursor.m_begin?strlen(cursor.m_begin):0,
                    (char const*)bdb_key.data,
                    bdb_key.size);
                if (!cursor.m_begin) break; ///<һ��ȡ��ǰֻ
                if (cursor.m_asc){///����desc��������ζ�Ҫprev
                    if (cursor.m_bBegin || ret<0) break; ///���������ʼֵ���ߵ���ֵ���ڳ�ʼֵ��������next
                }else{///����ǽ�������Ҫprev������
                    break;
                }
            }
            if (cursor.m_asc){
                ret = pCursor->m_cursor->get(pCursor->m_cursor, &bdb_key, &bdb_data, DB_NEXT);
            }else{
                ret = pCursor->m_cursor->get(pCursor->m_cursor, &bdb_key, &bdb_data, DB_PREV);
            }
            if (ret != 0){
                if (DB_NOTFOUND == ret) return 0;
                if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "Failure to move cursor to next. err-code:%d, err-msg=%s",
                    ret,
                    db_strerror(ret));
                return -1;
            }
        }while(0);

        if (cursor.m_end){
            ret = keyAsciiCmpLess((char const*)bdb_key.data,
                bdb_key.size,
                cursor.m_end,
                strlen(cursor.m_end));
            if (0 == ret) return 0;
            if (cursor.m_asc){
                if (0<ret) return 0;
            }else{
                if (0>ret) return 0;
            }
        }
        //set data
        cursor.m_uiStoreDataLen  = bdb_data.size;
        cursor.m_unStoreKeyLen = bdb_key.size;
        cursor.m_szStoreKey[bdb_key.size] = 0x00;
        return 1;
    }while(1);
    return 1;
}


///��Ϣ��ص�event������0���ɹ���-1��ʧ��
int UnistorStoreBdb::storeEvent(UnistorTss* tss, CwxMsgBlock*& msg)
{
    if (!m_bValid){
        strcpy(tss->m_szBuf2K, m_szErrMsg);
        return -1;
    }
    if (EVENT_STORE_COMMIT == msg->event().getEvent()){
        return _dealCommitEvent(tss, msg);
    }else if (EVENT_STORE_DEL_EXPIRE == msg->event().getEvent()){
        return _dealExpireEvent(tss, msg);
    }else if (EVENT_STORE_DEL_EXPIRE_REPLY == msg->event().getEvent()){
        return _dealExpireReplyEvent(tss, msg);
    }
    m_bValid = false;
    ///δ֪����Ϣ����
    CwxCommon::snprintf(m_szErrMsg, 2047, "Unknown event type:%u", msg->event().getEvent());
    strcpy(tss->m_szBuf2K, m_szErrMsg);
    return -1;
}


///�ر��α�
void UnistorStoreBdb::closeCursor(UnistorStoreCursor& cursor){
	UnistorStoreBdbCursor* pCursor = (UnistorStoreBdbCursor*) cursor.m_cursorHandle;
    if (pCursor->m_cursor){
        pCursor->m_cursor->close(pCursor->m_cursor);
    }
	delete pCursor;
    if (cursor.m_field){
        UnistorStoreBase::freeField(cursor.m_field);
        cursor.m_field = NULL;
    }
	cursor.m_cursorHandle = NULL;
}

///��ʼ�������ݡ�-1���ڲ�����ʧ�ܣ�0���ɹ�
int UnistorStoreBdb::exportBegin(UnistorStoreCursor& cursor,
                                 char const* szStartKey,
                                 char const* szExtra, ///<extra��Ϣ
                                 UnistorSubscribe const& scribe,
                                 CWX_UINT64& ullSid,
                                 char* szErr2K)
{
    if (!m_bdb){
        if (szErr2K) strcpy(szErr2K, m_szErrMsg);
        return -1;
    }
    UnistorStoreBdbCursor* pCursor =  new UnistorStoreBdbCursor();
    int ret = m_bdb->cursor(m_bdb, 0, &pCursor->m_cursor, DB_READ_UNCOMMITTED|DB_CURSOR_BULK);
    if (0 != ret){
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "Failure to create cursor. err-code:%d, err-msg=%s",
            ret,
            db_strerror(ret));
        delete pCursor;
        return -1;
    }

    if (szStartKey){
        cursor.m_strBeginKey = szStartKey;
    }else{
        cursor.m_strBeginKey.erase();
    }
    if (szExtra){
        cursor.m_strExtra = szExtra;
    }else{
        cursor.m_strExtra.erase();
    }
    cursor.m_scribe = scribe;
    if (!cursor.m_scribe.m_bAll &&
        (cursor.m_scribe.m_uiMode != UnistorSubscribe::SUBSCRIBE_MODE_MOD) &&
        (cursor.m_scribe.m_uiMode != UnistorSubscribe::SUBSCRIBE_MODE_RANGE) &&
        (cursor.m_scribe.m_uiMode != UnistorSubscribe::SUBSCRIBE_MODE_KEY))
    {
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "Invalid scribe mode:%u", cursor.m_scribe.m_uiMode);
        pCursor->m_cursor->close(pCursor->m_cursor);
        delete pCursor;
        return -1;
    }
    pCursor->m_cursor->set_priority(pCursor->m_cursor, DB_PRIORITY_VERY_LOW);
    pCursor->m_bFirst = true;
    cursor.m_cursorHandle = pCursor;
    ullSid = getCache()->getPrevCommitSid();
    return 0;

}
///��ȡ���ݡ�-1��ʧ�ܣ�0��������1����ȡһ����2��skip����Ϊ0
int UnistorStoreBdb::exportNext(UnistorTss* tss,
                                UnistorStoreCursor& cursor,
                                char const*& szKey,
                                CWX_UINT16& unKeyLen,
                                char const*& szData,
                                CWX_UINT32& uiDataLen,
                                bool& bKeyValue,
                                CWX_UINT32& uiVersion,
                                CWX_UINT32& uiExpire,
                                CWX_UINT16& unSkipNum,
                                char const*& szExtra,
                                CWX_UINT32& uiExtraLen)
{
    int ret = 0;
    szExtra = NULL;
    uiExtraLen = 0;
    if (cursor.m_scribe.m_bAll || 
        (cursor.m_scribe.m_uiMode == UnistorSubscribe::SUBSCRIBE_MODE_MOD) ||
        (cursor.m_scribe.m_uiMode != UnistorSubscribe::SUBSCRIBE_MODE_RANGE))
    {
        ret = _exportNext(tss, cursor, szKey, unKeyLen, szData, uiDataLen, bKeyValue, uiVersion, uiExpire, unSkipNum);
    }else if (cursor.m_scribe.m_uiMode != UnistorSubscribe::SUBSCRIBE_MODE_KEY){
        ret = _exportKeyNext(tss, cursor, szKey, unKeyLen, szData, uiDataLen, bKeyValue, uiVersion, uiExpire, unSkipNum);
    }else{
        CwxCommon::snprintf(tss->m_szBuf2K, 2047, "Invalid scribe mode:%u", cursor.m_scribe.m_uiMode);
        return -1;
    }
    if (1 == ret){
        ((char*)szKey)[unKeyLen] = 0x00;
    }
    return ret;
}

///������������
void UnistorStoreBdb::exportEnd(UnistorStoreCursor& cursor){
    UnistorStoreBdbCursor* pCursor = (UnistorStoreBdbCursor*) cursor.m_cursorHandle;
    if (pCursor){
        if (pCursor->m_cursor){
            pCursor->m_cursor->close(pCursor->m_cursor);
        }
        delete pCursor;
        cursor.m_cursorHandle = NULL;
    }
}


int UnistorStoreBdb::commit(char* szErr2K){
	return _commit(szErr2K);
}


///�رգ�0���ɹ���-1��ʧ��
int UnistorStoreBdb::close(){
	int ret = 0;
    if (getCache())getCache()->stop();
    if (m_bdbTxn){
        if ((ret = m_bdbTxn->commit(m_bdbTxn,0)) != 0){
            CWX_ERROR(("Failure to commit bdb, err-code=%d, err=%s", ret, db_strerror(ret)));
        }
        m_bdbTxn = NULL;
    }
	if (m_bdb){
		if ((ret = m_bdb->close(m_bdb, 0)) != 0){
			CWX_ERROR(("Failure to close bdb db, err-code=%d, err=%s", ret, db_strerror(ret)));
		}
		m_bdb = NULL;
	}
    if (m_sysDb){
        if ((ret = m_sysDb->close(m_sysDb, 0)) != 0){
            CWX_ERROR(("Failure to close bdb db, err-code=%d, err=%s", ret, db_strerror(ret)));
        }
        m_sysDb = NULL;
    }
    if (m_expireDb){
        if ((ret = m_expireDb->close(m_expireDb, 0)) != 0){
            CWX_ERROR(("Failure to close bdb db, err-code=%d, err=%s", ret, db_strerror(ret)));
        }
        m_expireDb = NULL;
    }
	if (m_bdbEnv){
		if ((ret = m_bdbEnv->close(m_bdbEnv, 0)) != 0) {
			fprintf(stderr, "Failure to close bdb env: err-code=%d, err=%s", ret, db_strerror(ret));
		}
		m_bdbEnv = NULL;
	}
    if (m_exKey){
        for (CWX_UINT32 i=0; i<UNISTOR_PER_FETCH_EXPIRE_KEY_NUM; i++){
            if (m_exKey[i].second) free(m_exKey[i].second);
        }
        delete [] m_exKey;
        m_exKey = NULL;
    }
    m_unExKeyNum = 0;
    m_unExKeyPos = 0;
    if (m_exFreeMsg.begin() != m_exFreeMsg.end()){
        list<CwxMsgBlock*>::iterator iter = m_exFreeMsg.begin();
        while(iter != m_exFreeMsg.end()){
            CwxMsgBlockAlloc::free(*iter);
            iter++;
        }
        m_exFreeMsg.clear();
    }
	return UnistorStoreBase::close();
}

///checkpoint
void UnistorStoreBdb::checkpoint()
{
	if (m_bdbEnv && m_bValid){
		m_bdbEnv->txn_checkpoint(m_bdbEnv, 0, 0, 0);
		char **list = NULL;
		m_bdbEnv->log_archive(m_bdbEnv, &list, DB_ARCH_REMOVE);
		if (list != NULL) {
			free(list);
		}
	}
}

///commit��0���ɹ���-1��ʧ��
int UnistorStoreBdb::_commit(char* szErr2K){
    if (!m_bValid){
        if (szErr2K) strcpy(szErr2K, m_szErrMsg);
        return -1;
    }
    if (0 != getCache()->commit(m_ullStoreSid, m_szErrMsg)){
        m_bValid = false;
        if (szErr2K) strcpy(szErr2K, m_szErrMsg);
        return -1;
    }
    m_uiUncommitBinlogNum = 0;
    ///��checkpoint�̷߳���commit��Ϣ
    if (m_pMsgPipeFunc){
        CwxMsgBlock* msg = CwxMsgBlockAlloc::malloc(0);
        msg->event().setEvent(EVENT_STORE_COMMIT);
        if (0 != m_pMsgPipeFunc(m_pMsgPipeApp, msg, false, szErr2K)){
            CwxMsgBlockAlloc::free(msg);
        }
    }
    return 0;
}

//0:�ɹ���-1��ʧ��
int UnistorStoreBdb::_updateSysInfo(DB_TXN* tid, CWX_UINT64 ullSid, char* szErr2K){
	if (!m_bValid){
		if (szErr2K) strcpy(szErr2K, m_szErrMsg);
		return -1;
	}
	//sid
	char szSid[64];
	char szKey[128];
	CwxCommon::toString(ullSid, szSid, 10);
    CWX_INFO(("Set bdb sid valus is %s", szSid));
    strcpy(szKey, UNISTOR_KEY_SID);
    if (0 != _setBdbKey(m_sysDb, tid, szKey, strlen(szKey), 128, szSid, strlen(szSid), 0, szErr2K)){
        return -1;
    }
	return 0;
}

//0:�ɹ���-1���ɹ�
int UnistorStoreBdb::_loadSysInfo(DB_TXN* tid, char* szErr2K){
    char szBuf[512];
    char szKey[128];
    CWX_UINT32 uiBufLen=511;

    ///��ȡUNISTOR_KEY_SID
    int ret = _getBdbKey(m_sysDb,
        NULL,
        UNISTOR_KEY_SID,
        strlen(UNISTOR_KEY_SID),
        szBuf,
        uiBufLen,
        szKey,
        128,
        0,
        m_szErrMsg);
    if (-1 == ret){
        CWX_ERROR(("Failure to get [%s] key, err=%s", UNISTOR_KEY_SID, m_szErrMsg));
        if (szErr2K) strcpy(szErr2K, m_szErrMsg);
        return -1;
    }else if (0 == ret){
        m_ullStoreSid = 0;
        CWX_INFO(("Not find bdb's sid[%s]", UNISTOR_KEY_SID));
    }else{
        szBuf[uiBufLen] = 0x00;
        m_ullStoreSid = strtoull(szBuf, NULL, 10);
        CWX_INFO(("Bdb's sid value is:%s", szBuf));
    }
    //��ȡUNISTOR_KEY_E
    uiBufLen=511;
    ret = _getBdbKey(m_sysDb,
        NULL,
        UNISTOR_KEY_E,
        strlen(UNISTOR_KEY_E),
        szBuf,
        uiBufLen,
        szKey,
        128,
        0,
        m_szErrMsg);
    if (-1 == ret){
        CWX_ERROR(("Failure to get [%s] key, err=%s", UNISTOR_KEY_E, m_szErrMsg));
        if (szErr2K) strcpy(szErr2K, m_szErrMsg);
        return -1;
    }else if (0 == ret){
        m_bEnableExpire = m_config->getCommon().m_bEnableExpire;
        CWX_INFO(("Not find bdb's expire[%s], insert it", UNISTOR_KEY_E));
        sprintf(szBuf, "%d", m_bEnableExpire?1:0);
        strcpy(szKey, UNISTOR_KEY_E);
        if (0 != _setBdbKey(m_sysDb, tid, szKey, strlen(szKey), 128, szBuf, strlen(szBuf), 0, m_szErrMsg)){
            CWX_ERROR(("Failure to set [%s] key, err=%s", UNISTOR_KEY_E, m_szErrMsg));
            if (szErr2K) strcpy(szErr2K, m_szErrMsg);
            return -1;
        }
    }else{
        szBuf[uiBufLen] = 0x00;
        m_bEnableExpire = strtoull(szBuf, NULL, 10)?true:false;
        CWX_INFO(("Bdb's expire value is:%s", m_bEnableExpire?"true":"false"));
    }
    if (m_bEnableExpire != m_config->getCommon().m_bEnableExpire){
        CwxCommon::snprintf(m_szErrMsg, 2048, "[%s] key in sys-db is [%d], but config is [%d]", UNISTOR_KEY_E, m_bEnableExpire?1:0, m_config->getCommon().m_bEnableExpire?1:0);
        CWX_ERROR((m_szErrMsg));
        if (szErr2K) strcpy(szErr2K, m_szErrMsg);
        return -1;
    }
    return 0;
}

//0:�ɹ���-1��ʧ��
int UnistorStoreBdb::_setKey(char const* szKey,
                             CWX_UINT16 unKeyLen,
                                char const* szData,
                                CWX_UINT32 uiLen,
                                CWX_UINT32 ttOldExpire,
                                bool bCache,
                                char* szErr2K)
{
    if (!m_bValid){
        if (szErr2K) strcpy(szErr2K, m_szErrMsg);
        return -1;
    }
    int ret = getCache()->updateKey(szKey, unKeyLen, szData, uiLen, ttOldExpire, bCache);
    if (0 == ret){
        if (-1 == _commit(szErr2K)) return -1;
        ret = getCache()->updateKey(szKey, unKeyLen, szData, uiLen, ttOldExpire, bCache);
    };
    if (-1 == ret){
        m_bValid = false;
        strcpy(m_szErrMsg, getCache()->getErrMsg());
        if (szErr2K) strcpy(szErr2K, m_szErrMsg);
        CWX_ERROR((m_szErrMsg));
        return -1;
    }else if (0 == ret){
        m_bValid = true;
        strcpy(m_szErrMsg, "UnistorCache::updateKey can't return 0 after commit");
        if (szErr2K) strcpy(szErr2K, m_szErrMsg);
        CWX_ERROR((m_szErrMsg));
        return -1;
    }else if (-2 == ret){
        CWX_ASSERT(0);
    }
    m_uiUncommitBinlogNum++;
    if (m_uiUncommitBinlogNum >= m_config->getCommon().m_uiStoreFlushNum){
        if (0 != _commit(szErr2K)) return -1;
    }
    return 0;
}


//0:�����ڣ�1����ȡ��-1��ʧ��
int UnistorStoreBdb::_getKey(char const* szKey,
                             CWX_UINT16 unKeyLen,
                                char* szData,
                                CWX_UINT32& uiLen,
                                char* szStoreKeyBuf,
                                CWX_UINT16 unKeyBufLen,
                                bool bCache,
                                char* szErr2K)
{
    if (!m_bdb&&!m_bValid){
        if (szErr2K) strcpy(szErr2K, m_szErrMsg);
        return -1;
    }
    int ret = 0;
    bool bDel = false;
    ret = getCache()->getKey(szKey, unKeyLen, szData, uiLen, bDel);
    if (-1 == ret){
        if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "Data buf size[%u] is too small.", uiLen);
        return -1;
    }else if (1 == ret){
        return bDel?0:1;
    }
    ret =  _getBdbKey(m_bdb, NULL, szKey, unKeyLen, szData, uiLen, szStoreKeyBuf, unKeyBufLen, DB_READ_UNCOMMITTED, szErr2K);
    //cache����
    if ((1 == ret) && bCache){
        getCache()->cacheKey(szKey, unKeyLen, szData, uiLen, true);
    }
    return ret;
}


//0:�ɹ���-1��ʧ��
int UnistorStoreBdb::_delKey(char const* szKey,
                             CWX_UINT16 unKeyLen,
                             CWX_UINT32 ttOldExpire,
                             char* szErr2K)
{
    if (!m_bValid){
        if (szErr2K) strcpy(szErr2K, m_szErrMsg);
        return -1;
    }
    int ret = getCache()->delKey(szKey, unKeyLen, ttOldExpire);
    if (0 == ret){
        if (-1 == _commit(szErr2K)) return -1;
        ret = getCache()->delKey(szKey, unKeyLen, ttOldExpire);
    };
    if (-1 == ret){
        m_bValid = false;
        strcpy(m_szErrMsg, getCache()->getErrMsg());
        if (szErr2K) strcpy(szErr2K, m_szErrMsg);
        CWX_ERROR((m_szErrMsg));
        return -1;
    }else if (0 == ret){
        m_bValid = false;
        strcpy(m_szErrMsg, "UnistorCache::updateKey can't return 0 after commit");
        if (szErr2K) strcpy(szErr2K, m_szErrMsg);
        CWX_ERROR((m_szErrMsg));
        return -1;
    }
    m_uiUncommitBinlogNum++;
    if (m_uiUncommitBinlogNum >= m_config->getCommon().m_uiStoreFlushNum){
        if (0 != _commit(szErr2K)) return -1;
    }
    return 0;
}


//0:�ɹ���-1��ʧ��
int UnistorStoreBdb::_setBdbKey(DB* db,
                                DB_TXN* tid,
                                char const* szKey,
                                CWX_UINT16 unKeyLen,
                                CWX_UINT16 unKeyBufLen,
                                char const* szData,
                                CWX_UINT32 uiDataLen,
                                CWX_UINT32 flags,
                                char* szErr2K)
{
	if (!m_bValid){
		if (szErr2K) strcpy(szErr2K, m_szErrMsg);
		return -1;
	}
    int ret = 0;
    DBT bdb_key;
    memset(&bdb_key, 0x00, sizeof(bdb_key));
    bdb_key.size = unKeyLen;
    bdb_key.data = (void*)szKey;
    bdb_key.flags=DB_DBT_USERMEM;
    bdb_key.ulen = unKeyBufLen;
	DBT bdb_data;
	memset(&bdb_data, 0, sizeof(DBT));
	bdb_data.data = (void*)szData;
	bdb_data.size = uiDataLen;
	ret = db->put(db, tid, &bdb_key, &bdb_data, flags);
	if (0 != ret){
        CwxCommon::snprintf(m_szErrMsg, 2047, "Failure to put key to bdb, key=%s, err-code:%d, err-msg=%s",
            szKey,
            ret,
            db_strerror(ret));
        CWX_ERROR((szErr2K));
		if (szErr2K) {
            if (szErr2K) strcpy(szErr2K, m_szErrMsg);
		}
		if ((DB_LOCK_DEADLOCK!=ret)){
			m_bValid = false;
		}
		return -1;
	}
	return 0;
}


//0:�����ڣ�1����ȡ��-1��ʧ��
int UnistorStoreBdb::_getBdbKey(DB* db,
                                DB_TXN* tid,
                                char const* szKey,
                                CWX_UINT16 unKeyLen,
                                char* szData,
                                CWX_UINT32& uiLen,
                                char* szStoreKeyBuf,
                                CWX_UINT16 unKeyBufLen,
                                CWX_UINT32 flags,
                                char* szErr2K)
{
	if (!m_bdb&&!m_bValid){
		if (szErr2K) strcpy(szErr2K, m_szErrMsg);
		return -1;
	}
    int ret = 0;
    DBT bdb_key;
    memset(&bdb_key, 0x00, sizeof(bdb_key));
    bdb_key.size = unKeyLen;
    memcpy(szStoreKeyBuf, szKey, unKeyLen);
    bdb_key.data = (void*)szStoreKeyBuf;
    bdb_key.flags=DB_DBT_USERMEM;
    bdb_key.ulen = unKeyBufLen;
	DBT bdb_data;
	memset(&bdb_data, 0, sizeof(bdb_data));
	bdb_data.data = szData;
	bdb_data.ulen = uiLen;
	bdb_data.flags = DB_DBT_USERMEM;
    {
        ret = db->get(db, tid, &bdb_key, &bdb_data, flags);
        if (0 == ret){
            uiLen = bdb_data.size;
            return 1;
        }else if (DB_NOTFOUND == ret){
            return 0;
        }
    }
	if (szErr2K){
		CwxCommon::snprintf(szErr2K, 2047, "Failure to fetch bdb key:%s, err-code:%d, err-msg=%s",
			szKey,
			ret,
			db_strerror(ret));
		CWX_ERROR((szErr2K));
	}else{
		CWX_ERROR(("Failure to fetch bdb key:%s, err-code:%d, err-msg=%s",
			szKey,
			ret,
			db_strerror(ret)));
	}
	return -1;
}
//0:�ɹ���-1��ʧ��
int UnistorStoreBdb::_delBdbKey(DB* db,
                                DB_TXN* tid,
                                char const* szKey,
                                CWX_UINT16 unKeyLen,
                                CWX_UINT16 unKeyBufLen,
                                CWX_UINT32 flags,
                                char* szErr2K)
{
	if (!m_bValid){
		if (szErr2K) strcpy(szErr2K, m_szErrMsg);
		return -1;
	}
    int ret  = 0;
    DBT bdb_key;
    memset(&bdb_key, 0x00, sizeof(bdb_key));
    bdb_key.size = unKeyLen;
    bdb_key.data = (void*)szKey;
    bdb_key.flags=DB_DBT_USERMEM;
    bdb_key.ulen = unKeyBufLen;
    //delete from bdb
    ret = db->del(db, tid, &bdb_key, flags);
    if ((0 == ret)||(DB_NOTFOUND==ret)){
        return 0;
    }
    CwxCommon::snprintf(m_szErrMsg, 2047, "Failure to del key from bdb, key=%s, err-code:%d, err-msg=%s",
        szKey,
        ret,
        db_strerror(ret));
    CWX_ERROR((szErr2K));
    if (szErr2K) {
        if (szErr2K) strcpy(szErr2K, m_szErrMsg);
    }
    if ((DB_LOCK_DEADLOCK!=ret)){
        m_bValid = false;
    }
    return -1;
}

int UnistorStoreBdb::_cmp( DBT* key1,  DBT* key2){
    return unistor_bdb_compare(m_bdb, key1, key2);
}

//����commit�¼���0���ɹ���-1��ʧ��
int UnistorStoreBdb::_dealCommitEvent(UnistorTss* tss, CwxMsgBlock*& )
{
    CWX_UINT32 i=0;
    if (!m_config->getCommon().m_bEnableExpire) return 0; ///<�����ⳬʱ
    if (m_unExKeyPos < m_unExKeyNum) return 0; ///<����δ������ĳ�ʱ
    if (!m_exKey){
        m_exKey = new pair<CWX_UINT32, UnistorStoreExpireKey*>[UNISTOR_PER_FETCH_EXPIRE_KEY_NUM];
        for (i=0; i<UNISTOR_PER_FETCH_EXPIRE_KEY_NUM; i++){
            m_exKey[i].second = (UnistorStoreExpireKey*)malloc(sizeof(UnistorStoreExpireKey) + UNISTOR_MAX_KEY_SIZE);
            m_exKey[i].first = 0;
        }
        m_unExKeyNum = 0;
        for (i=0; i<m_config->getCommon().m_uiExpireConcurrent; i++){
            m_exFreeMsg.push_back(CwxMsgBlockAlloc::malloc(UNISTOR_MAX_KEY_SIZE));
        }
    }
    int ret =_loadExpireData(tss, false); 
    if (-1 == ret) return -1;
    if (0 == ret) return 0;
    return _sendExpireData(tss);
}

//���س�ʱ�����ݡ�0��û�������ݣ�1����ȡ�����ݣ�-1��ʧ��
int UnistorStoreBdb::_loadExpireData(UnistorTss* tss, bool bJustContinue){
    if (bJustContinue){///�����¼���
        if (m_unExKeyNum < UNISTOR_PER_FETCH_EXPIRE_KEY_NUM) return 0;
    }
    CwxKeyValueItem key;
    CWX_UINT32 uiVersion=0;
    CWX_UINT32 uiFieldNum=0;
    int iRet = 0;
    static DBC*  cursor=NULL;
    DBT bdb_key;
    DBT bdb_data;

    memset(&bdb_key, 0x00, sizeof(bdb_key));
    memset(&bdb_data, 0x00, sizeof(bdb_data));

    bdb_key.data = (void*)m_exStoreKey;
    bdb_key.flags=DB_DBT_USERMEM;
    bdb_key.ulen = UNISTOR_MAX_KEY_SIZE;

    bdb_data.data = (void*)tss->m_szBuf2K;
    bdb_data.ulen = 2048;
    bdb_data.flags = DB_DBT_USERMEM;
    if (!cursor){
        m_expireDb->cursor(m_expireDb, 0, &cursor, DB_READ_UNCOMMITTED);
        if ((iRet = cursor->get(cursor, &bdb_key, &bdb_data, DB_FIRST)) != 0){
            ///�ر�cursor
            cursor->close(cursor);
            cursor = NULL;
            if (DB_NOTFOUND == iRet) return 0;
            m_bValid = false;
            CwxCommon::snprintf(m_szErrMsg, 2047, "Failure to move cursor to expire db's begin. err-code:%d, err-msg=%s",
                iRet,
                db_strerror(iRet));
            strcpy(tss->m_szBuf2K, m_szErrMsg);
            CWX_ERROR((m_szErrMsg));
            return -1;
        }
    }else{
        if ((iRet = cursor->get(cursor, &bdb_key, &bdb_data, DB_NEXT)) != 0){
            ///�ر�cursor
            cursor->close(cursor);
            cursor = NULL;
            if (DB_NOTFOUND == iRet) return 1;
            m_bValid = false;
            CwxCommon::snprintf(m_szErrMsg, 2047, "Failure to move cursor to expire db's key. err-code:%d, err-msg=%s",
                iRet,
                db_strerror(iRet));
            strcpy(tss->m_szBuf2K, m_szErrMsg);
            CWX_ERROR((m_szErrMsg));
            return -1;
        }
    }
    //��ȡkey
    m_unExKeyNum = 0;
    m_unExKeyPos = 0;
    while(m_unExKeyNum < UNISTOR_PER_FETCH_EXPIRE_KEY_NUM){
        m_exKey[m_unExKeyNum].first = bdb_key.size - sizeof(UnistorStoreExpireKey);
        memcpy(m_exKey[m_unExKeyNum].second, bdb_key.data, bdb_key.size);
        m_exKey[m_unExKeyNum].second->m_key[m_exKey[m_unExKeyNum].first] = 0x00;
        if (m_exKey[m_unExKeyNum].second->m_ttExpire >= m_ttExpireClock) break;
        if (0 == m_unExKeyNum){
            CWX_INFO(("Begin load expired key:%s", m_exKey[m_unExKeyNum].second->m_key));
        }
        //���Լ���key�����write�̵߳�Ч��
        key.m_szData = (char*)m_exKey[m_unExKeyNum].second->m_key;
        key.m_uiDataLen = m_exKey[m_unExKeyNum].first;
        isExist(tss, key, NULL, NULL, uiVersion, uiFieldNum);
        m_unExKeyNum++;
        if ((iRet = cursor->get(cursor, &bdb_key, &bdb_data, DB_NEXT)) != 0){
            ///�ر�cursor
            cursor->close(cursor);
            cursor = NULL;
            if (DB_NOTFOUND == iRet) return 1;
            m_bValid = false;
            CwxCommon::snprintf(m_szErrMsg, 2047, "Failure to move cursor to expire db's key. err-code:%d, err-msg=%s",
                iRet,
                db_strerror(iRet));
            strcpy(tss->m_szBuf2K, m_szErrMsg);
            CWX_ERROR((m_szErrMsg));
            return -1;
        }
    }
    if (m_unExKeyNum){
        CWX_INFO(("End load expired key:%s", m_exKey[m_unExKeyNum-1].second->m_key));
    }
    return 1;
}

int UnistorStoreBdb::_sendExpireData(UnistorTss* tss){
    int iRet = 0;
    CwxMsgBlock* msg=NULL;
    while(m_exFreeMsg.begin() != m_exFreeMsg.end()){
        if (m_unExKeyPos >= m_unExKeyNum){
            iRet = _loadExpireData(tss, true);
            if (-1 == iRet) return -1;
            if (0 == iRet) break;
        }
        msg = *m_exFreeMsg.begin();
        msg->reset();
        memcpy(msg->wr_ptr(), m_exKey[m_unExKeyPos].second->m_key, m_exKey[m_unExKeyPos].first);
        msg->wr_ptr()[m_exKey[m_unExKeyPos].first]=0x00;
        msg->wr_ptr(m_exKey[m_unExKeyPos].first);
        msg->event().setEvent(EVENT_STORE_DEL_EXPIRE);
        msg->event().setTimestamp(m_exKey[m_unExKeyPos].second->m_ttExpire);
        if (0 != m_pMsgPipeFunc(m_pMsgPipeApp, msg, true, tss->m_szBuf2K)){
            m_bValid = false;
            CWX_ERROR(("Failure to send expired key to write thread. err=%s", tss->m_szBuf2K));
            strcpy(m_szErrMsg, tss->m_szBuf2K);
            return -1;
        }
        m_exFreeMsg.pop_front();
        m_unExKeyPos++;
    }
    return 0;
}

//����expire�¼���0���ɹ���-1��ʧ��
int UnistorStoreBdb::_dealExpireEvent(UnistorTss* tss, CwxMsgBlock*& msg)
{
    CWX_UINT32 uiVersion = 0;
    CWX_UINT32 ttOldExpire = 0;
    CWX_UINT32 uiBufLen = UNISTOR_MAX_KV_SIZE;
    char* szBuf = tss->getBuf(uiBufLen);
    int ret = _getKey(msg->rd_ptr(), msg->length(), szBuf, uiBufLen, tss->m_szStoreKey, UNISTOR_MAX_KEY_SIZE, false, tss->m_szBuf2K);
    if (-1 == ret){
        m_bValid = false;
        strcpy(m_szErrMsg, tss->m_szBuf2K);
        CWX_ERROR(("Failure to get expire key, err:%s", tss->m_szBuf2K));
        return -1;
    }
    if (1 == ret){
        getKvVersion(szBuf, uiBufLen, ttOldExpire, uiVersion);
        if (ttOldExpire == msg->event().getTimestamp()){///ͬһ��key
            if (0 != _delKey(msg->rd_ptr(), msg->length(), ttOldExpire, tss->m_szBuf2K)) return -1;
        }
    }
    msg->event().setEvent(EVENT_STORE_DEL_EXPIRE_REPLY);
    if (0 != m_pMsgPipeFunc(m_pMsgPipeApp, msg, false, tss->m_szBuf2K)){
        m_bValid = false;
        CWX_ERROR(("Failure to send expired key to write thread. err=%s", tss->m_szBuf2K));
        strcpy(m_szErrMsg, tss->m_szBuf2K);
        return -1;
    }
    msg = NULL;
    return 0;
}

//����expire�¼��Ļظ���0���ɹ���-1��ʧ��
int UnistorStoreBdb::_dealExpireReplyEvent(UnistorTss* tss, CwxMsgBlock*& msg){
    m_exFreeMsg.push_back(msg);
    msg = NULL;
    return _sendExpireData(tss);
}

//����modģʽ�Ķ��ġ�-1��ʧ�ܣ�0��������1����ȡһ����2��skip����Ϊ0
int UnistorStoreBdb::_exportNext(UnistorTss* tss,
                                 UnistorStoreCursor& cursor,
                                 char const*& szKey,
                                 CWX_UINT16& unKeyLen,
                                 char const*& szData,
                                 CWX_UINT32& uiDataLen,
                                 bool& bKeyValue,
                                 CWX_UINT32& uiVersion,
                                 CWX_UINT32& uiExpire,
                                 CWX_UINT16& unSkipNum)
{
    UnistorStoreBdbCursor* pCursor = (UnistorStoreBdbCursor*)cursor.m_cursorHandle;
    DBT bdb_key;
    DBT bdb_data;
    int ret = 0;
    CWX_UINT32 uiGroupId = 0;
    do{
        if (!unSkipNum) return 2;
        memset(&bdb_key, 0x00, sizeof(bdb_key));
        memset(&bdb_data, 0x00, sizeof(bdb_data));        
        bdb_key.data = (void*)cursor.m_szStoreKey;
        bdb_key.flags=DB_DBT_USERMEM;
        bdb_key.ulen = UNISTOR_MAX_KEY_SIZE;
        szKey = cursor.m_szStoreKey;
        bdb_data.data = (void*)cursor.m_szStoreData;
        bdb_data.ulen = UNISTOR_MAX_KV_SIZE;
        bdb_data.flags = DB_DBT_USERMEM;
        szData = cursor.m_szStoreData;
        if (pCursor->m_bFirst){///��һ��
            pCursor->m_bFirst = false;
            cursor.m_unCacheKeyLen = cursor.m_strBeginKey.length();
            if (cursor.m_unCacheKeyLen >= UNISTOR_MAX_KEY_SIZE){
                cursor.m_unCacheKeyLen = UNISTOR_MAX_KEY_SIZE -1;
            }
            if (cursor.m_unCacheKeyLen){
                memcpy(cursor.m_szCacheKey, cursor.m_strBeginKey.c_str(), cursor.m_unCacheKeyLen);
            }
            cursor.m_szCacheKey[cursor.m_unCacheKeyLen] = 0x00;
            if (cursor.m_szCacheKey[0]){
                bdb_key.size = cursor.m_unCacheKeyLen;
                strcpy(cursor.m_szStoreKey, cursor.m_szCacheKey);
                if ((ret = pCursor->m_cursor->get(pCursor->m_cursor, &bdb_key, &bdb_data, DB_SET_RANGE)) == 0){
                    if (keyAsciiCmpLess((char const*)bdb_key.data,
                        bdb_key.size,
                        cursor.m_szCacheKey,
                        cursor.m_unCacheKeyLen) == 0)
                    {///��һ��
                        ret = pCursor->m_cursor->get(pCursor->m_cursor, &bdb_key, &bdb_data, DB_NEXT);
                    }
                }
            }else{
                ret = pCursor->m_cursor->get(pCursor->m_cursor, &bdb_key, &bdb_data, DB_FIRST);
            }
        }else{
            ret = pCursor->m_cursor->get(pCursor->m_cursor, &bdb_key, &bdb_data, DB_NEXT);
        }

        if (0 == ret){
            uiDataLen = bdb_data.size;
            getKvVersion(szData, uiDataLen, uiExpire, uiVersion);
            if (m_config->getCommon().m_bEnableExpire && (uiExpire<=m_ttExpireClock)){
                unSkipNum--;
                continue;
            }
            if (!cursor.m_scribe.m_bAll){
                uiGroupId = getKeyGroup(cursor.m_szStoreKey, bdb_key.size);
                if (UnistorSubscribe::SUBSCRIBE_MODE_MOD == cursor.m_scribe.m_uiMode){
                    if (!cursor.m_scribe.m_mod.isSubscribe(uiGroupId)){
                        unSkipNum--;
                        continue;
                    }
                }else{
                    if (!cursor.m_scribe.m_range.isSubscribe(uiGroupId)){
                        unSkipNum--;
                        continue;
                    }
                }
            }
            unKeyLen = bdb_key.size;
            cursor.m_szStoreKey[unKeyLen] = 0x00;
            bKeyValue = isKvData(szData, uiDataLen);
            uiDataLen -= getKvDataSignLen();
            unSkipNum--;
            return 1;
        }else if (DB_NOTFOUND == ret){
            return 0;
        }else{
            CwxCommon::snprintf(tss->m_szBuf2K, 2047, "Failure to move cursor. err-code:%d, err-msg=%s",
                ret,
                db_strerror(ret));
            return -1;
        }
    }while(1);
    return 0;
}


//����keyģʽ�Ķ��ġ�-1��ʧ�ܣ�0��������1����ȡһ����2��skip����Ϊ0
int UnistorStoreBdb::_exportKeyNext(UnistorTss* tss,
                                    UnistorStoreCursor& cursor,
                                    char const*& szKey,
                                    CWX_UINT16& unKeyLen,
                                    char const*& szData,
                                    CWX_UINT32& uiDataLen,
                                    bool& bKeyValue,
                                    CWX_UINT32& uiVersion,
                                    CWX_UINT32& uiExpire,
                                    CWX_UINT16& unSkipNum)
{
    UnistorStoreBdbCursor* pCursor = (UnistorStoreBdbCursor*)cursor.m_cursorHandle;
    DBT bdb_key;
    DBT bdb_data;
    int ret = 0;
    bool  bSeek = false; ///�Ƿ����¶�λ
    bool  bNext = pCursor->m_bFirst;
    do{
        if (!unSkipNum) return 2;
        memset(&bdb_key, 0x00, sizeof(bdb_key));
        memset(&bdb_data, 0x00, sizeof(bdb_data));        
        bdb_key.data = (void*)cursor.m_szStoreKey;
        bdb_key.flags=DB_DBT_USERMEM;
        bdb_key.ulen = UNISTOR_MAX_KEY_SIZE;
        szKey = cursor.m_szStoreKey;
        bdb_data.data = (void*)cursor.m_szStoreData;
        bdb_data.ulen = UNISTOR_MAX_KV_SIZE;
        bdb_data.flags = DB_DBT_USERMEM;
        szData = cursor.m_szStoreData;
        if (pCursor->m_bFirst || bSeek){///��һ��
            ///����keyģʽ���޶�begin��ֵ
            string strKey= pCursor->m_bFirst?cursor.m_strBeginKey:cursor.m_szCacheKey;
            string strBegin;
            string strEnd;
            pCursor->m_bFirst = false;
            if (!_exportKeyInit(strKey, strBegin, strEnd, cursor.m_scribe.m_key)){
                return 0;
            }
            if (strBegin.length() > UNISTOR_MAX_KEY_SIZE - 1){
                memcpy(cursor.m_szCacheKey, strBegin.c_str(), UNISTOR_MAX_KEY_SIZE - 1);
                cursor.m_szCacheKey[UNISTOR_MAX_KEY_SIZE - 1] = 0x00;
                cursor.m_unCacheKeyLen = UNISTOR_MAX_KEY_SIZE - 1;
            }else if (!strBegin.length()){
                cursor.m_szCacheKey[0]=0x00;
                cursor.m_unCacheKeyLen = 0;
            }else{
                strcpy(cursor.m_szCacheKey, strBegin.c_str());
                cursor.m_unCacheKeyLen = strBegin.length();
            }
            if (strEnd.length() > UNISTOR_MAX_KEY_SIZE - 1){
                memcpy(cursor.m_szCacheData, strEnd.c_str(), UNISTOR_MAX_KEY_SIZE - 1);
                cursor.m_uiCacheDataLen = UNISTOR_MAX_KEY_SIZE - 1;
                cursor.m_szCacheData[UNISTOR_MAX_KEY_SIZE - 1] = 0x00;
            }else if (!strEnd.length()){
                cursor.m_szCacheData[0]=0x00;
                cursor.m_uiCacheDataLen = 0;
            }else{
                strcpy(cursor.m_szCacheData, strEnd.c_str());
                cursor.m_uiCacheDataLen = strEnd.length();
            }
            if (cursor.m_szCacheKey[0]){
                bdb_key.size = cursor.m_unCacheKeyLen;
                strcpy(cursor.m_szStoreKey, cursor.m_szCacheKey);
                if ((ret = pCursor->m_cursor->get(pCursor->m_cursor, &bdb_key, &bdb_data, DB_SET_RANGE)) == 0){
                    if (bNext &&
                        (keyAsciiCmpLess((char const*)bdb_key.data, bdb_key.size, cursor.m_szCacheKey, strlen(cursor.m_szCacheKey)) == 0))
                    {///��һ��
                        bNext = false;
                        ret = pCursor->m_cursor->get(pCursor->m_cursor, &bdb_key, &bdb_data, DB_NEXT);
                    }
                }
            }else{
                ret = pCursor->m_cursor->get(pCursor->m_cursor, &bdb_key, &bdb_data, DB_FIRST);
            }
        }else{
            ret = pCursor->m_cursor->get(pCursor->m_cursor, &bdb_key, &bdb_data, DB_NEXT);
        }
        if (0 == ret){
            uiDataLen = bdb_data.size;
            getKvVersion(szData, uiDataLen, uiExpire, uiVersion);
            if (m_config->getCommon().m_bEnableExpire && (uiExpire<=m_ttExpireClock)){
                unSkipNum--;
                continue;
            }
            unKeyLen = bdb_key.size;
            cursor.m_szStoreKey[unKeyLen] = 0x00;
            if (cursor.m_szCacheData[0]){///������unSkipNum--
                if (keyAsciiCmpLess(szKey, unKeyLen, cursor.m_szCacheData, cursor.m_uiCacheDataLen) >= 0){///�Ѿ�����
                    strcpy(cursor.m_szCacheKey, szKey);
                    cursor.m_unCacheKeyLen = strlen(szKey);
                    bSeek = true;
                    continue;
                }
            }
            bKeyValue = isKvData(szData, uiDataLen);
            uiDataLen -= getKvDataSignLen();
            unSkipNum--;
            return 1;
        }else if(DB_NOTFOUND == ret){
            return 0;
        }else{
            CwxCommon::snprintf(tss->m_szBuf2K, 2047, "Failure to move cursor. err-code:%d, err-msg=%s",
                ret,
                db_strerror(ret));
            return -1;
        }
    }while(1);
    return 0;
}

bool UnistorStoreBdb::_exportKeyInit(string const& strKeyBegin,
                                     string& strBegin,
                                     string& strEnd,
                                     UnistorSubscribeKey const& keys)
{
    map<string,string>::const_iterator iter =  keys.m_keys.begin();
    while(iter != keys.m_keys.end()){
        if ((strKeyBegin < iter->second) || (!iter->second.length())){
            ///����begin
            if (strKeyBegin <= iter->first){
                strBegin = iter->first;
            }else{
                strBegin = strKeyBegin;
            }
            ///����end
            strEnd = iter->second;
            return true;
        }
        iter++;
    }
    return false;
}