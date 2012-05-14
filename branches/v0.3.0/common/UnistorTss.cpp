#include "UnistorTss.h"

///Îö¹¹º¯Êý
UnistorTss::~UnistorTss(){
    if (m_pZkConf) delete m_pZkConf;
    if (m_pZkLock) delete m_pZkLock;
    if (m_pReader) delete m_pReader;
    if (m_pItemReader) delete m_pItemReader;
    if (m_pEngineReader) delete m_pEngineReader;
    if (m_pEngineItemReader) delete m_pEngineItemReader;
    if (m_pWriter) delete m_pWriter;
    if (m_pItemWriter) delete m_pItemWriter;
    if (m_pEngineWriter) delete m_pEngineWriter;
    if (m_pEngineItemWriter) delete m_pEngineItemWriter;
    if (m_szDataBuf) delete [] m_szDataBuf;
    if (m_szDataBuf1) delete [] m_szDataBuf1;
    if (m_szDataBuf2) delete [] m_szDataBuf2;
    if (m_szDataBuf3) delete [] m_szDataBuf3;
    if (m_userObj) delete m_userObj;
}

int UnistorTss::init(UnistorTssUserObj* pUserObj){
    m_pZkConf = NULL;
    m_pZkLock = NULL;
    m_pReader = new CwxPackageReader(false);
    m_pItemReader = new CwxPackageReader(false);
    m_pEngineReader = new CwxPackageReader(false);
    m_pEngineItemReader = new CwxPackageReader(false);
    m_pWriter = new CwxPackageWriter(UNISTOR_DEF_KV_SIZE);
    m_pItemWriter = new CwxPackageWriter(UNISTOR_DEF_KV_SIZE);
    m_pEngineWriter = new CwxPackageWriter(UNISTOR_DEF_KV_SIZE);
    m_pEngineItemWriter = new CwxPackageWriter(UNISTOR_DEF_KV_SIZE);
    m_szDataBuf = new char[UNISTOR_DEF_KV_SIZE];
    m_uiDataBufLen= UNISTOR_DEF_KV_SIZE;
    m_userObj = pUserObj;
    return 0;
}
