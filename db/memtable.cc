// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/memtable.h"
#include "db/dbformat.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "util/coding.h"
#include <sstream>
#include <fstream>
#include <unordered_set>
#include "rapidjson/document.h"
#include "db_impl.h"


namespace leveldb {

static Slice GetLengthPrefixedSlice(const char* data) {
  uint32_t len;
  const char* p = data;
  p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted
  return Slice(p, len);
}

MemTable::MemTable(const InternalKeyComparator& cmp, std::string secAtt)
    : comparator_(cmp),
      refs_(0),
      table_(comparator_, &arena_) {
    secAttribute = secAtt;
}

MemTable::~MemTable() {
  assert(refs_ == 0);
  for (SecMemTable::iterator it = secTable_.begin() ; it != secTable_.end() ; it++)
    {

        pair<string, vector<string>*> pr  =  *it;

        vector<string>* invertedList = pr.second;
        invertedList->clear();
        delete invertedList;
    }
}

size_t MemTable::ApproximateMemoryUsage() { return arena_.MemoryUsage(); }

int MemTable::KeyComparator::operator()(const char* aptr, const char* bptr)
    const {
  // Internal keys are encoded as length-prefixed strings.
  Slice a = GetLengthPrefixedSlice(aptr);
  Slice b = GetLengthPrefixedSlice(bptr);
  return comparator.Compare(a, b);
}

// Encode a suitable internal key target for "target" and return it.
// Uses *scratch as scratch space, and the returned pointer will point
// into this scratch space.
static const char* EncodeKey(std::string* scratch, const Slice& target) {
  scratch->clear();
  PutVarint32(scratch, target.size());
  scratch->append(target.data(), target.size());
  return scratch->data();
}

class MemTableIterator: public Iterator {
 public:
  explicit MemTableIterator(MemTable::Table* table) : iter_(table) { }

  virtual bool Valid() const { return iter_.Valid(); }
  virtual void Seek(const Slice& k) { iter_.Seek(EncodeKey(&tmp_, k)); }
  virtual void SeekToFirst() { iter_.SeekToFirst(); }
  virtual void SeekToLast() { iter_.SeekToLast(); }
  virtual void Next() { iter_.Next(); }
  virtual void Prev() { iter_.Prev(); }
  virtual Slice key() const { return GetLengthPrefixedSlice(iter_.key()); }
  virtual Slice value() const {
    Slice key_slice = GetLengthPrefixedSlice(iter_.key());
    return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
  }

  virtual Status status() const { return Status::OK(); }

 private:
  MemTable::Table::Iterator iter_;
  std::string tmp_;       // For passing to EncodeKey

  // No copying allowed
  MemTableIterator(const MemTableIterator&);
  void operator=(const MemTableIterator&);
};

Iterator* MemTable::NewIterator() {
  return new MemTableIterator(&table_);
}

void MemTable::Add(SequenceNumber s, ValueType type,
                   const Slice& key,
                   const Slice& value) {
  // Format of an entry is concatenation of:
  //  key_size     : varint32 of internal_key.size()
  //  key bytes    : char[internal_key.size()]
  //  value_size   : varint32 of value.size()
  //  value bytes  : char[value.size()]
  size_t key_size = key.size();
  size_t val_size = value.size();
  size_t internal_key_size = key_size + 8;
  const size_t encoded_len =
      VarintLength(internal_key_size) + internal_key_size +
      VarintLength(val_size) + val_size;
  char* buf = arena_.Allocate(encoded_len);
  char* p = EncodeVarint32(buf, internal_key_size);
  memcpy(p, key.data(), key_size);
  p += key_size;
  EncodeFixed64(p, (s << 8) | type);
  p += 8;
  p = EncodeVarint32(p, val_size);
  memcpy(p, value.data(), val_size);
  assert((p + val_size) - buf == encoded_len);
  table_.Insert(buf);
  
  ////SECONDARY MEMTABLE
  if(type == kTypeDeletion)
      return;
  rapidjson::Document docToParse; 

  docToParse.Parse<0>(value.ToString().c_str());   

    ///
    const char* sKeyAtt = secAttribute.c_str();

    if(!docToParse.IsObject()||!docToParse.HasMember(sKeyAtt)||docToParse[sKeyAtt].IsNull())
        return;

    std::ostringstream skey;
    if(docToParse[sKeyAtt].IsNumber())
    {
          if(docToParse[sKeyAtt].IsUint64())
          {
              unsigned long long int tid = docToParse[sKeyAtt].GetUint64();
              skey<<tid;

          }
          else if (docToParse[sKeyAtt].IsInt64())
          {
              long long int tid = docToParse[sKeyAtt].GetInt64();
              skey<<tid;
          }
          else if (docToParse[sKeyAtt].IsDouble())
          {
              double tid = docToParse[sKeyAtt].GetDouble();
              skey<<tid;
          }

          else if (docToParse[sKeyAtt].IsUint())
          {
              unsigned int tid = docToParse[sKeyAtt].GetUint();
              skey<<tid;
          }
          else if (docToParse[sKeyAtt].IsInt())
          {
              int tid = docToParse[sKeyAtt].GetInt();
             skey<<tid;             
          }
    }
    else if (docToParse[sKeyAtt].IsString())
    {
          const char* tid = docToParse[sKeyAtt].GetString();
          skey<<tid;
    }
    else if(docToParse[sKeyAtt].IsBool())
    {
          bool tid = docToParse[sKeyAtt].GetBool();
          skey<<tid;
    }

    std::string secKey = skey.str();
    SecMemTable::const_iterator lookup = secTable_.find(secKey);
    if (lookup == secTable_.end()) {
        
        vector<string> *invertedList = new vector<string>();
        invertedList->push_back(key.ToString());

         
        secTable_.insert(std::make_pair( secKey , invertedList));
    }

    else {
        pair<string, vector<string> *> pr  =  *lookup;
        pr.second->push_back(key.ToString());
        
    }
    
  
}

bool MemTable::Get(const LookupKey& key, std::string* value, Status* s) {
  Slice memkey = key.memtable_key();
  Table::Iterator iter(&table_);
  iter.Seek(memkey.data());
  if (iter.Valid()) {
    // entry format is:
    //    klength  varint32
    //    userkey  char[klength]
    //    tag      uint64
    //    vlength  varint32
    //    value    char[vlength]
    // Check that it belongs to same user key.  We do not check the
    // sequence number since the Seek() call above should have skipped
    // all entries with overly large sequence numbers.
    const char* entry = iter.key();
    uint32_t key_length;
    const char* key_ptr = GetVarint32Ptr(entry, entry+5, &key_length);
    if (comparator_.comparator.user_comparator()->Compare(
            Slice(key_ptr, key_length - 8),
            key.user_key()) == 0) {
      // Correct user key
      const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
      switch (static_cast<ValueType>(tag & 0xff)) {
        case kTypeValue: {
          Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
          value->assign(v.data(), v.size());
          return true;
        }
        case kTypeDeletion:
          *s = Status::NotFound(Slice());
          return true;
      }
    }
  }
  return false;
}
//SECONDARY MEMTABLE
bool MemTable::Get(const LookupKey& key, std::string* value, Status* s, uint64_t *tag) {
  Slice memkey = key.memtable_key();
  Table::Iterator iter(&table_);
  iter.Seek(memkey.data());
  if (iter.Valid()) {
    // entry format is:
    //    klength  varint32
    //    userkey  char[klength]
    //    tag      uint64
    //    vlength  varint32
    //    value    char[vlength]
    // Check that it belongs to same user key.  We do not check the
    // sequence number since the Seek() call above should have skipped
    // all entries with overly large sequence numbers.
    const char* entry = iter.key();
    uint32_t key_length;
    const char* key_ptr = GetVarint32Ptr(entry, entry+5, &key_length);
    if (comparator_.comparator.user_comparator()->Compare(
            Slice(key_ptr, key_length - 8),
            key.user_key()) == 0) {
      // Correct user key
      *tag = DecodeFixed64(key_ptr + key_length - 8);
      switch (static_cast<ValueType>(*tag & 0xff)) {
        case kTypeValue: {
          Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
          value->assign(v.data(), v.size());
          return true;
        }
        case kTypeDeletion:
          *s = Status::NotFound(Slice());
          return true;
      }
    }
  }
  return false;
}
//SECONDARY MEMTABLE
void MemTable::Get(const Slice& skey, SequenceNumber snapshot, std::vector<SKeyReturnVal>* value, Status* s,  std::unordered_set<std::string>* resultSetofKeysFound, int topKOutput )
{
  
    auto lookup = secTable_.find(skey.ToString());
    if (lookup != secTable_.end()) 
    {

        pair<string, vector<string>* > pr  =  *lookup;
        for(int i = pr.second->size()-1 ;i>=0; i--)
        {
            if(value->size()>= topKOutput)
                return;
                
            Slice pkey = pr.second->at(i) ;
            LookupKey lkey(pkey, snapshot);
            std::string svalue;
            Status s;
            uint64_t tag;
            if(this->Get(lkey, &svalue, &s, &tag))
            {
                if(!s.IsNotFound())
                {
                    rapidjson::Document docToParse; 

                    docToParse.Parse<0>(svalue.c_str());   

                      ///
                    const char* sKeyAtt = secAttribute.c_str();

                    if(!docToParse.IsObject()||!docToParse.HasMember(sKeyAtt)||docToParse[sKeyAtt].IsNull())
                        return;

                    std::ostringstream tempskey;
                    if(docToParse[sKeyAtt].IsNumber())
                    {
                          if(docToParse[sKeyAtt].IsUint64())
                          {
                              unsigned long long int tid = docToParse[sKeyAtt].GetUint64();
                              tempskey<<tid;

                          }
                          else if (docToParse[sKeyAtt].IsInt64())
                          {
                              long long int tid = docToParse[sKeyAtt].GetInt64();
                              tempskey<<tid;
                          }
                          else if (docToParse[sKeyAtt].IsDouble())
                          {
                              double tid = docToParse[sKeyAtt].GetDouble();
                              tempskey<<tid;
                          }

                          else if (docToParse[sKeyAtt].IsUint())
                          {
                              unsigned int tid = docToParse[sKeyAtt].GetUint();
                              tempskey<<tid;
                          }
                          else if (docToParse[sKeyAtt].IsInt())
                          {
                              int tid = docToParse[sKeyAtt].GetInt();
                             tempskey<<tid;             
                          }
                    }
                    else if (docToParse[sKeyAtt].IsString())
                    {
                          const char* tid = docToParse[sKeyAtt].GetString();
                          tempskey<<tid;
                    }
                    else if(docToParse[sKeyAtt].IsBool())
                    {
                          bool tid = docToParse[sKeyAtt].GetBool();
                          tempskey<<tid;
                    }

                    Slice valsKey = tempskey.str();
                    if (comparator_.comparator.user_comparator()->Compare(
                    valsKey,
                    skey) == 0) {
                    struct SKeyReturnVal newVal;
                    newVal.key = pr.second->at(i);
                    std::string temp;

                    if(resultSetofKeysFound->find(newVal.key)==resultSetofKeysFound->end())
                    {
                   
                        newVal.value = svalue; 
                        newVal.sequence_number = tag;



                        if(value->size()<topKOutput)
                        {
                            
                            newVal.Push(value, newVal);
                            resultSetofKeysFound->insert(newVal.key);

                        }
                        else if(newVal.sequence_number>value->front().sequence_number)
                        {
                             
                            newVal.Pop(value);
                            newVal.Push(value,newVal);
                            resultSetofKeysFound->insert(newVal.key);
                            resultSetofKeysFound->erase(resultSetofKeysFound->find(value->front().key));
                            
                        }
                         
                        
                         

                    }

                  }
                }
            }
        }
        
    }
  
  
  
}  

  
} // namespace leveldb
