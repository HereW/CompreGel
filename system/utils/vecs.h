#ifndef VECS_H_
#define VECS_H_

#include "serialization.h"
#include "Combiner.h"
#include "global.h"
#include <vector>
using namespace std;

template <class KeyT, class MessageT>
struct msgpair
{
    KeyT key;
    MessageT msg;

    msgpair()
    {
    }

    msgpair(KeyT v1, MessageT v2)
    {
        key = v1;
        msg = v2;
    }

    inline bool operator<(const msgpair &rhs) const
    {
        return key < rhs.key;
    }
};

template <class KeyT, class MessageT>
ibinstream &operator<<(ibinstream &m, const msgpair<KeyT, MessageT> &v)
{
    m << v.key;
    m << v.msg;
    return m;
}

template <class KeyT, class MessageT>
obinstream &operator>>(obinstream &m, msgpair<KeyT, MessageT> &v)
{
    m >> v.key;
    m >> v.msg;
    return m;
}

//===============================================

template <class KeyT, class MessageT, class HashT, class HashIdxT>
class Vecs
{
public:
    typedef vector<msgpair<KeyT, MessageT>> Vec;
    typedef vector<Vec> VecGroup;

    typedef vector<KeyT> KeyVec;
    typedef vector<KeyVec> KeyVecGroup;
    typedef vector<MessageT> MsgVec;
    typedef vector<MsgVec> MsgVecGroup;

    int np;
    VecGroup vecs;
    HashT hash;
    HashIdxT hashIdx;

    KeyVecGroup keyVecs;
    MsgVecGroup msgVecs;

    Vecs()
    {
        int np = _num_workers;
        this->np = np;
        vecs.resize(np);
        keyVecs.resize(np);
        msgVecs.resize(np);
    }

    // void append(const KeyT key, const MessageT msg)
    // {
    //     msgpair<KeyT, MessageT> item(key, msg);
    //     vecs[hash(key)].push_back(item);
    // }

    void append(const KeyT key, const MessageT msg)
    {
        msgpair<KeyT, MessageT> item(hashIdx(key), msg);
        vecs[hash(key)].push_back(item);
    }

    Vec &getBuf(int pos)
    {
        return vecs[pos];
    }

    VecGroup &getBufs()
    {
        return vecs;
    }

    KeyVec &getKeyBuf(int pos)
    {
        return keyVecs[pos];
    }

    MsgVec &getMsgBuf(int pos)
    {
        return msgVecs[pos];
    }

    KeyVecGroup &getKeyBufs()
    {
        return keyVecs;
    }

    MsgVecGroup &getMsgBufs()
    {
        return msgVecs;
    }

    void clear()
    {
        for (int i = 0; i < np; i++)
        {
            vecs[i].clear();
        }
    }

    //============================
    // apply combiner logic

    void combine()
    {
        Combiner<MessageT> *combiner = (Combiner<MessageT> *)get_combiner();
        for (int i = 0; i < np; i++)
        {
            sort(vecs[i].begin(), vecs[i].end());
            Vec newVec;
            int size = vecs[i].size();
            if (size > 0)
            {
                newVec.push_back(vecs[i][0]);
                KeyT preKey = vecs[i][0].key;
                for (int j = 1; j < size; j++)
                {
                    msgpair<KeyT, MessageT> &cur = vecs[i][j];
                    if (cur.key != preKey)
                    {
                        newVec.push_back(cur);
                        preKey = cur.key;
                    }
                    else
                    {
                        combiner->combine(newVec.back().msg, cur.msg);
                    }
                }
            }
            newVec.swap(vecs[i]);
        }
    }

    // void getMsgVecs()
    // {
    //     for (int i = 0; i < np; i++) {
    //         MsgVec newMsgVec;
    //         int size = vecs[i].size();
    //         if (size > 0) {
    //             KeyT preKey = vecs[i][0].key;
    //             newMsgVec.push_back(preKey);
    //             newMsgVec.push_back(vecs[i][0].msg);
    //             for (int j = 1; j < size; j++) {
    //                 msgpair<KeyT, MessageT>& cur = vecs[i][j];
    //                 if (cur.key - preKey > 1) {
    //                     newMsgVec.push_back(cur.key - preKey - 1);
    //                     newMsgVec.push_back(cur.msg);
    //                     preKey = cur.key;
    //                 } else {
    //                     newMsgVec.push_back(cur.msg);
    //                     preKey++;
    //                 }
    //             }
    //         }
    //         newMsgVec.swap(msgVecs[i]);
    //     }
    // }

    // void getMsgVecs()
    // {
    //     for (int i = 0; i < np; i++) {
    //         MsgVec newMsgVec;
    //         MsgVec newMsgVec2;
    //         int size = vecs[i].size();
    //         if (size > 0) {
    //             KeyT preKey = vecs[i][0].key;
    //             newMsgVec.push_back(preKey);
    //             newMsgVec2.push_back(vecs[i][0].msg);
    //             for (int j = 1; j < size; j++) {
    //                 msgpair<KeyT, MessageT>& cur = vecs[i][j];
    //                 if (cur.key - preKey > 1) {
    //                     newMsgVec.push_back(cur.key - preKey - 1);
    //                     newMsgVec2.push_back(cur.msg);
    //                     preKey = cur.key;
    //                 } else {
    //                     newMsgVec2.push_back(cur.msg);
    //                     preKey++;
    //                 }
    //             }
    //         }
    //         newMsgVec.swap(msgVecs[i]);
    //         newMsgVec2.swap(msgVecs2[i]);
    //     }
    // }
    void getMsgVecs()
    {
        for (int i = 0; i < np; i++)
        {
            KeyVec newKeyVec;
            MsgVec newMsgVec;

            int size = vecs[i].size();
            if(size==0){
                newKeyVec.swap(keyVecs[i]);
                newMsgVec.swap(msgVecs[i]);
                continue;
            }
            //encode
            newKeyVec.assign((vecs[i][size - 1].key + 1) / 32 + 1, 0);
            for (int j = 0; j < size; j++)
            {
                msgpair<KeyT, MessageT> &cur = vecs[i][j];
                // newMsgVec.push_back(cur.key);
                int byteIndex = cur.key / 32;
                int bitIndex = cur.key % 32;
                //while(byteIndex>newKeyVec.size()-1)
                //    newKeyVec.push_back(0);
                newKeyVec[byteIndex] |= (1 << bitIndex);
                newMsgVec.push_back(cur.msg);
            }
            newKeyVec.swap(keyVecs[i]);
            newMsgVec.swap(msgVecs[i]);
        }
    }

    // void getMsgVecs()
    // {
    //     for (int i = 0; i < np; i++) {
    //         MsgVec newMsgVec;
    //         int size = vecs[i].size();
    //         if (size > 0) {
    //             KeyT preKey = vecs[i][0].key;
    //             newMsgVec.push_back(vecs[i][0].msg + preKey);
    //             for (int j = 1; j < size; j++) {
    //                 msgpair<KeyT, MessageT>& cur = vecs[i][j];
    //                 if (cur.key - preKey > 1) {
    //                     newMsgVec.push_back(cur.key - preKey - 1 + cur.msg);
    //                     preKey = cur.key;
    //                 } else {
    //                     newMsgVec.push_back(cur.msg);
    //                     preKey++;
    //                 }
    //             }
    //         }
    //         newMsgVec.swap(msgVecs[i]);
    //     }
    // }

    long long get_total_msg()
    {
        long long sum = 0;
        for (int i = 0; i < vecs.size(); i++)
        {
            sum += vecs[i].size();
        }
        return sum;
    }
};

#endif
