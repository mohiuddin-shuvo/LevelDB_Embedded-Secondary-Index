#include <stdio.h>
#include <stdlib.h>
#include <cassert>
#include <leveldb/db.h>
#include <leveldb/filter_policy.h>
#include <fstream>
#include <iostream>

using namespace std;


int main() {
    leveldb::DB *db;
    leveldb::Options options;
    options.filter_policy = leveldb::NewBloomFilterPolicy(10);
    options.PrimaryAtt = "ID";
    options.secondaryAtt = "UserID";
    options.create_if_missing = true;
    leveldb::Status status = leveldb::DB::Open(options, "/Users/nakshikatha/Desktop/test codes/leveldb_dir_temp", &db);
    assert(status.ok());
    
    ifstream ifile("/Users/nakshikatha/Desktop/test codes/tweet_extract.txt");
    //ofstream ofile("tweet_extract.txt");
    if (!ifile) { cerr << "Can't open input file " << endl; return -1; }
    string line;
    int i=0;
    while(getline(ifile, line))
    {
        //cout<<line<<endl;
        char outString[15];
        leveldb::WriteOptions woptions;
        sprintf(outString,"%d tweet",i);
        db->Put(woptions,outString,line);
        
        db->Put(woptions,line);
    }
    /*
    for(i=0;i<=100000;i++)
    {
        leveldb::WriteOptions woptions;
        char outString[15];
        
        sprintf(outString,"%d Hello",i);
        leveldb::Slice s= outString;
        char outString2[15];
        
        
        
        sprintf(outString2,"%d World",i);
        
        
        leveldb::Slice t = outString2;
        
        db->Put(woptions, s, t);
        //printf("Writing <hello, world>%d\n",i);
        
    }*/
    /*
    leveldb::WriteOptions woptions;
    leveldb::Slice s = "hello";
    leveldb::Slice t = "World";

    printf("Writing <hello, world>\n");
    
    
    db->Put(woptions, s, t);
    leveldb::WriteOptions woptions1;
    leveldb::Slice s1 = "hi";
    leveldb::Slice t1 = "leveldb";
    db->Put(woptions1, s1, t1);
    printf("Writing <hello, world>\n");
    leveldb::WriteOptions woptions2;
    leveldb::Slice s2 = "test";
    leveldb::Slice t2 = "code";
    db->Put(woptions2, s2, t2);
    printf("Writing <hello, world>\n");
    
    */
    
    
    delete db;
    delete options.filter_policy;
    return 0;
}
