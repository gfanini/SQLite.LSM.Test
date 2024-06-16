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
#include "lsmint.h"

using namespace std;
// compiled as 32 bit need /LARGEADDRESSAWARE linker option for MapViewOfFile > 2 gb when LSM_CONFIG_MMAP is on

#define TOTAL_WRITES  20e6 
#define NO_WRITES_TRANSACTION (TOTAL_WRITES/10) //50000 // no writes in transaction
#define RECORD_SIZE 100

void runInserts(lsm_db *db, int start, int end) {
  char Key[128];
  char Val[512];
  int rc, bytes;
  //long long keyLength = 0;
  //long long valueLength = 0;
  LARGE_INTEGER sum = { 0 };
  LARGE_INTEGER pcFreq = { 0 };
  int recordsinserted = 0;

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
    sprintf(Val, "value:%d", i+1000);//  valuess << "value" << i+1000 << endl;
    //}

    //string sKey = keyss.str();
    //string sVal = valuess.str();
    //const char *pKey = sKey.c_str();
    int nKey = strlen(Key);// sKey.length();
    //const char *pVal = sVal.c_str();
    int nVal = RECORD_SIZE;// strlen(Val); //sVal.length();

    
    while (lsm_insert(db, Key, nKey, Val, nVal) == LSM_BUSY) {
      failedAttempts++;
      Sleep(1);
    }
    recordsinserted++;
    /*if (!(i % 10000)) {
        int nOld, nLive;
        rc = lsm_info(db, LSM_INFO_TREE_SIZE, &nOld, &nLive);

        //rc = lsm_flush(db);
        //bytes = 0;
        //rc = lsm_checkpoint(db, &bytes);
        //Sleep(5000);
    }*/
    //keyLength += nKey;
    //valueLength += nVal;
  }
  rc=lsm_commit(db, 0);
  if(failedAttempts)
    cout << "Failed attempts " << failedAttempts << " !" << endl;


  rc = lsm_flush(db);
  bytes = 0;
  //rc = lsm_checkpoint(db, &bytes);
  //rc=lsm_work(db, 1, -1, 0);// merges all segments into one.i.e.it rewrites the entire database to be a single optimized b - tree.

  QueryPerformanceCounter(&stop);
  sum.QuadPart += (stop.QuadPart - start1.QuadPart);

  double timeTaken = double(sum.QuadPart) / freq;
  cout << "records " << end << " inserts/s " << (double)(recordsinserted * 1000 / timeTaken) << " seconds " << timeTaken / 1000.0 << endl;

}


void insertionThread(lsm_db *lpParameter)
{
  lsm_db *db = (lsm_db *)lpParameter;
  
  for (int i = 0; i <= TOTAL_WRITES; i+= NO_WRITES_TRANSACTION) {
    runInserts(db, i, i + (NO_WRITES_TRANSACTION -1));
  }

}

void readerThread(lsm_db *db, int id) {
    LARGE_INTEGER sum = { 0 };
    LARGE_INTEGER pcFreq = { 0 };

    QueryPerformanceFrequency(&pcFreq);
    double freq = double(pcFreq.QuadPart) / 1000;
    LARGE_INTEGER start1, stop;
    QueryPerformanceCounter(&start1);

    //for (int i = 0; i < 100; i++) {
    //int skip = rand() % 1000;
    int totalkeysfound = 0;
    lsm_cursor *csr;
    if (lsm_csr_open(db, &csr) != LSM_OK) {
      return;
    }
    
    //Sleep(rand() % 100);

    stringstream keyprefix;
    keyprefix << "key:";// << skip;
    string startkey = keyprefix.str();
    //int seekrc = lsm_csr_seek(csr, startkey.c_str(), startkey.length() - 1, /*LSM_SEEK_EQ */ LSM_SEEK_LE);
    int seekrc = lsm_csr_first(csr);
    int validrc = lsm_csr_valid(csr);
    if ( seekrc == LSM_OK  && validrc) {
    //while (lsm_csr_next(csr) == LSM_OK /* && lsm_csr_valid(csr)*/) {
      do {
/*            char* pKey, * pVal;
            int nKey, nVal;
            lsm_csr_key(csr, (const void**)&pKey, &nKey);
            lsm_csr_value(csr, (const void**)&pVal, &nVal);
            char actualKey[128];// [nKey + 2] ;
            memcpy(actualKey, pKey, nKey);
            actualKey[nKey] = 0;
            char actualVal[512];
            memcpy(actualVal, pVal, nVal);
            actualVal[nVal] = 0;*/
            totalkeysfound++;
            //cout << " - " << id << " Found key " << actualKey << " val =" << actualVal << endl;
        } while (lsm_csr_next(csr) == LSM_OK && lsm_csr_valid(csr));
    }

    lsm_csr_close(csr);
    QueryPerformanceCounter(&stop);
    sum.QuadPart += (stop.QuadPart - start1.QuadPart);
    double timetaken=double(sum.QuadPart) / freq;

    cout << "total keys read " << totalkeysfound << " reads/s " << (double)(totalkeysfound*1000.0/timetaken) << endl;
  //}
}

int main()
{
  int rc;
  lsm_db* db;// , * db2;

  /* Allocate a new database handle */
  rc = lsm_new(0, &db);
  if (rc != LSM_OK) {
    exit(1);
  }

  int ro = 0;
  rc = lsm_config(db, LSM_CONFIG_READONLY, &ro);
  int iVal = 0;
  //lsm_config(db, LSM_CONFIG_USE_LOG, &iVal);
  //iVal = 4096;
  //lsm_config(db, LSM_CONFIG_AUTOFLUSH, &iVal);
  //iVal = 8192;
  //lsm_config(db, LSM_CONFIG_AUTOCHECKPOINT, &iVal);
  iVal = 32768;
  lsm_config(db, LSM_CONFIG_MMAP, &iVal);
  //iVal = 2;
  //lsm_config(db, LSM_CONFIG_AUTOMERGE, &iVal);

  iVal = 0;
  lsm_config(db, LSM_CONFIG_MULTIPLE_PROCESSES, &iVal);

  /* Connect the database handle to database "test.db" */
  rc = lsm_open(db, "test.lsmdb");
  if (rc != LSM_OK) {
    cout << "open failed code " << rc << endl;
    exit(1);
  }

  /* Allocate a new database handle 
  rc = lsm_new(0, &db2);
  if (rc != LSM_OK) {
    exit(1);
  }*/

  /*int ro = 0;
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
  */
  /* Connect the database handle to database "test.db" 
  rc = lsm_open(db2, "test.lsmdb");
  if (rc != LSM_OK) {
    exit(1);
  }*/

  thread t1(insertionThread, db);
  
  t1.join();
  
  thread t2(readerThread, db, 1);
  t2.join();

  //rc = lsm_close(db2);
//  rc = lsm_work(db, 1, -1, 0);// merges all segments into one.i.e.it rewrites the entire database to be a single optimized b - tree.
  rc = lsm_flush(db);
  //rc = lsmBeginWork(db);
  //rc = lsmFsIntegrityCheck(db);
  /*lsmSortedDumpStructure(
      db,                    // Database handle (used for xLog callback) 
      db->pWorker,                // Snapshot to dump 
      1,                      // Output the keys from each segment 
      1,                      // Output the values from each segment 
      "dump of db");                // Caption to print near top of dump 

  lsmFinishWork(db, 0, &rc);
  if (rc == LSM_OK)
      printf ("db is ok\n");
  else
      printf ("db error %d\n",rc);*/
  rc = lsm_close(db);
  return 0;
}


