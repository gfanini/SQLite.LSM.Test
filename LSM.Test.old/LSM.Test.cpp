// LSM.Test.cpp : Defines the entry point for the console application.
//

//#include "stdafx.h"
#include "lsm.h"

#include <Windows.h>
#include <string>       // std::string
#include <iostream>     // std::cout
#include <sstream>      // std::stringstream
#include <iomanip>
#include <thread>

using namespace std;

double runInserts(lsm_db *db, int start, int end) {
  char Key[128];
  char Val[512];
  int rc, bytes;
  //long long keyLength = 0;
  //long long valueLength = 0;
  LARGE_INTEGER sum = { 0 };
  LARGE_INTEGER pcFreq = { 0 };
  
  QueryPerformanceFrequency(&pcFreq);
  double freq = double(pcFreq.QuadPart) / 1000;
  ULONG failedAttempts = 0;
  LARGE_INTEGER start1, stop;

  QueryPerformanceCounter(&start1);

  rc = lsm_begin(db, 1);

  for (int i = start; i < end; i++) {
    //stringstream keyss;
    //stringstream valuess;
  
    //GUID guid;
    //CoCreateGuid(&guid);

    sprintf(Key, "key:%d", i );//<< ":" << guid.Data1 << "-" << guid.Data2 << "-" << guid.Data3;
    //for (int j = 0; j < 128; j++) {
    sprintf(Val, "value:%d______________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________", i+1000);//  valuess << "value" << i+1000 << endl;
    //}

    //string sKey = keyss.str();
    //string sVal = valuess.str();
    //const char *pKey = sKey.c_str();
    int nKey = strlen(Key);// sKey.length();
    //const char *pVal = sVal.c_str();
    int nVal = strlen(Val); //sVal.length();

    
    while (lsm_insert(db, Key, nKey, Val, nVal) == LSM_BUSY) {
      failedAttempts++;
      Sleep(1);
    }
    
    if (!(i % 10000)) {
        int nOld, nLive;
        rc = lsm_info(db, LSM_INFO_TREE_SIZE, &nOld, &nLive);

        //rc = lsm_flush(db);
        //bytes = 0;
        //rc = lsm_checkpoint(db, &bytes);
        //Sleep(5000);
    }
    //keyLength += nKey;
    //valueLength += nVal;
  }
  rc=lsm_commit(db, 0);
  if(failedAttempts)
    cout << "Failed attempts " << failedAttempts << " !" << endl;


  //rc = lsm_flush(db);
  bytes = 0;
  //rc = lsm_checkpoint(db, &bytes);
  rc=lsm_work(db, 1, -1, 0);// merges all segments into one.i.e.it rewrites the entire database to be a single optimized b - tree.

  QueryPerformanceCounter(&stop);
  sum.QuadPart += (stop.QuadPart - start1.QuadPart);

return double(sum.QuadPart) / freq;
}

#define TOTAL 1e6
#define CHUNKS 10000

void insertionThread(lsm_db *lpParameter)
{
  lsm_db *db = (lsm_db *)lpParameter;
  
  for (int i = 0; i < TOTAL; i+= CHUNKS) {
    double timeTaken = runInserts(db, i, i + (CHUNKS-1));
    cout << " inserts/s " << (double)(CHUNKS *1000/timeTaken) << " seconds " << timeTaken/1000.0 << endl;
  }

}

void readerThread(lsm_db *db, int id) {
  //for (int i = 0; i < 100; i++) {
    //int skip = rand() % 1000;
    lsm_cursor *csr;
    if (lsm_csr_open(db, &csr) != LSM_OK) {
      return;
    }
    
    //Sleep(rand() % 100);

    stringstream keyprefix;
    keyprefix << "key:";// << skip;
    string startkey = keyprefix.str();
    int seekrc = lsm_csr_seek(csr, startkey.c_str(), startkey.length() - 1, LSM_SEEK_GE);
    int validrc = lsm_csr_valid(csr);
    if ( seekrc == LSM_OK && validrc) {
        do {

            char* pKey, * pVal;
            int nKey, nVal;
            lsm_csr_key(csr, (const void**)&pKey, &nKey);
            lsm_csr_value(csr, (const void**)&pVal, &nVal);
            char actualKey[128];// [nKey + 2] ;
            memcpy(actualKey, pKey, nKey);
            actualKey[nKey] = 0;
            char actualVal[512];
            memcpy(actualVal, pVal, nVal);
            actualVal[nVal] = 0;

            //cout << " - " << id << " Found key " << actualKey << " val =" << actualVal << endl;
        } while (lsm_csr_next(csr)== LSM_OK && lsm_csr_valid(csr));
    }

    lsm_csr_close(csr);
  //}
}

int main()
{
  int rc;
  lsm_db *db, *db2;

  /* Allocate a new database handle */
  rc = lsm_new(0, &db);
  if (rc != LSM_OK) {
    exit(1);
  }

  /* Connect the database handle to database "test.db" */
  rc = lsm_open(db, "test.lsmdb");
  if (rc != LSM_OK) {
    exit(1);
  }

  /* Allocate a new database handle */
  rc = lsm_new(0, &db2);
  if (rc != LSM_OK) {
    exit(1);
  }

  int ro = 0;
  rc = lsm_config(db2, LSM_CONFIG_READONLY, &ro);
  int iVal = 0;
  lsm_config(db2, LSM_CONFIG_USE_LOG, &iVal);
  iVal = 4096;
  lsm_config(db2, LSM_CONFIG_AUTOFLUSH, &iVal);
  iVal = 8192;
  lsm_config(db2, LSM_CONFIG_AUTOCHECKPOINT, &iVal);
  //iVal = 1;
  //lsm_config(db2, LSM_CONFIG_MMAP, &iVal);
  iVal = 2;
  lsm_config(db2, LSM_CONFIG_AUTOMERGE, &iVal);

  /* Connect the database handle to database "test.db" */
  rc = lsm_open(db2, "test.lsmdb");
  if (rc != LSM_OK) {
    exit(1);
  }

  thread t1(insertionThread, db);
  thread t2(readerThread, db2, 1);
  t1.join();
  
  
  t2.join();

  rc = lsm_close(db);
  rc = lsm_close(db2);
  return 0;
}


