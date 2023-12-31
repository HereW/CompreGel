#ifndef MESSAGEBUFFER_H
#define MESSAGEBUFFER_H

#include <vector>
#include "../utils/global.h"
#include "../utils/Combiner.h"
#include "../utils/communication.h"
#include "../utils/vecs.h"
using namespace std;

template <class VertexT>
class MessageBuffer {
public:
    typedef typename VertexT::KeyType KeyT;
    typedef typename VertexT::MessageType MessageT;
    typedef typename VertexT::HashType HashT;
    typedef typename VertexT::HashIdxType HashIdxT;
    typedef vector<MessageT> MessageContainerT;
    typedef hash_map<KeyT, int> Map; //int = position in v_msg_bufs //CHANGED FOR VADD
    typedef Vecs<KeyT, MessageT, HashT, HashIdxT> VecsT;
    typedef typename VecsT::Vec Vec;
    typedef typename VecsT::VecGroup VecGroup;
    typedef typename VecsT::MsgVec MsgVec;
    typedef typename VecsT::KeyVec KeyVec;
    typedef typename Map::iterator MapIter;

    VecsT out_messages;
    Map in_messages;
    vector<VertexT*> to_add;
    vector<MessageContainerT> v_msg_bufs;
    HashT hash;

    void init(vector<VertexT*> vertexes)
    {
        // int me = get_worker_id();
        v_msg_bufs.resize(vertexes.size());
        for (int i = 0; i < vertexes.size(); i++) {
            VertexT* v = vertexes[i];
            // if ((me == 1) && (i < 200))
            //     std::cout << v->id << " ";
            // std::cout << "proc_id: " << me << "  id: " << v->id << " ";
            in_messages[v->id] = i; //CHANGED FOR VADD
        }
        // if (me == 1)
        //     std::cout << "\n";
    }
    void reinit(vector<VertexT*> vertexes)
    {
        v_msg_bufs.resize(vertexes.size());
	    in_messages.clear();
        for (int i = 0; i < vertexes.size(); i++) {
            VertexT* v = vertexes[i];
            in_messages[v->id] = i; //CHANGED FOR VADD
        }
    }
    void add_message(const KeyT& id, const MessageT& msg)
    {
        hasMsg(); //cannot end yet even every vertex halts
        out_messages.append(id, msg);
    }

    Map& get_messages()
    {
        return in_messages;
    }

    void combine()
    {
        //apply combiner
        Combiner<MessageT>* combiner = (Combiner<MessageT>*)get_combiner();
        if (combiner != NULL)
            out_messages.combine();
        out_messages.getMsgVecs();
    }

    // void getMsgVecs(vector<VertexT*> vertexes) {
        
    //     out_messages.getMsgVecs(vertexes);
    // }

    vector<VertexT*>& sync_messages(double SZerror)
    {
        int np = get_num_workers();
        int me = get_worker_id();

        //------------------------------------------------
        // get messages from remote
        vector<vector<VertexT*> > add_buf(_num_workers);
        //set send buffer
        for (int i = 0; i < to_add.size(); i++) {
            VertexT* v = to_add[i];
            add_buf[hash(v->id)].push_back(v);
        }
        //================================================
        //exchange msgs
        //exchange vertices to add
        // all_to_all_cat(out_messages.getMsgBufs(), add_buf);
        // all_to_all_cat(out_messages.getBufs(), add_buf);
        // all_to_all_cat(out_messages.getBufs(), out_messages.getMsgBufs2());
        // all_to_all_cat(out_messages.getKeyBufs(), out_messages.getMsgBufs());
        // if (step_num() < 5) {
        // printf("proc id: %d, total msg:%ld\n", me, get_total_msg());
        if (get_total_msg() < 1e5) {
            all_to_all_cat_ori(out_messages.getKeyBufs(), out_messages.getMsgBufs());
        }
        else {
            all_to_all_cat(out_messages.getKeyBufs(), out_messages.getMsgBufs(), SZerror);
        }

        //------------------------------------------------
        //delete sent vertices
        for (int i = 0; i < to_add.size(); i++) {
            VertexT* v = to_add[i];
            if (hash(v->id) != _my_rank)
                delete v;
        }
        to_add.clear();
        //collect new vertices to add
        for (int i = 0; i < np; i++) {
            to_add.insert(to_add.end(), add_buf[i].begin(), add_buf[i].end());
        }

        //================================================
        //Change of G33
        int oldsize = v_msg_bufs.size();
        v_msg_bufs.resize(oldsize + to_add.size());
        for (int i = 0; i < to_add.size(); i++) {
            int pos = oldsize + i;
            in_messages[to_add[i]->id] = pos; //CHANGED FOR VADD
        }

        //================================================
        // gather all messages
        // for (int i = 0; i < np; i++) {
        //     Vec& msgBuf = out_messages.getBuf(i);
        //     for (int i = 0; i < msgBuf.size(); i++) {
        //         MapIter it = in_messages.find(msgBuf[i].key);
        //         if (it != in_messages.end()) //filter out msgs to non-existent vertices
        //             v_msg_bufs[it->second].push_back(msgBuf[i].msg); //CHANGED FOR VADD
        //     }
        // }
        // gather all messages
        // for (int i = 0; i < np; i++) {
        //     Vec& msgBuf = out_messages.getBuf(i);
        //     for (int i = 0; i < msgBuf.size(); i++) {
        //         v_msg_bufs[msgBuf[i].key].push_back(msgBuf[i].msg); //CHANGED FOR VADD
        //     }
        // }
        // // gather all messages
        // for (int i = 0; i < np; i++) {
        //     MsgVec& msgBuf = out_messages.getMsgBuf(i);
        //     int size = msgBuf.size();
        //     if (size > 0) {
        //         int curIdx = int(msgBuf[0]);
        //         v_msg_bufs[curIdx].push_back(msgBuf[1]);
        //         curIdx++;
        //         for (int j = 2; j < size; j++) {
        //             if (msgBuf[j] >= 1.0 - 1E-4) {
        //                 curIdx += int(msgBuf[j] + 1E-4);
        //             }
        //             else {
        //                 v_msg_bufs[curIdx].push_back(msgBuf[j]);
        //                 curIdx++;
        //             }
        //         }
        //     }
        // }
        // gather all messages
        // decode
        for (int i = 0; i < np; i++) {
            KeyVec& keyBuf = out_messages.getKeyBuf(i);
            MsgVec& msgBuf = out_messages.getMsgBuf(i);
            int size = keyBuf.size();
            int size2 = msgBuf.size();
            int tt = 0;
            for (int j = 0; j < size; j++) {
                int val=keyBuf[j];
                for(int k=0;k<32;k++){
                    if(val&1){
                        // v_msg_bufs[j*32+k].push_back(std::max(msgBuf[tt++], 0.0));//28 13:27
                        v_msg_bufs[j*32+k].push_back(msgBuf[tt++]);//28 13:27
                    }
                    val>>=1;
                }
                if(tt>=size2) break;
            }
        }
        // // gather all messages
        // for (int i = 0; i < np; i++) {
        //     MsgVec& msgBuf = out_messages.getMsgBuf(i);
        //     int size = msgBuf.size();
        //     if (size > 0) {
        //         msgBuf[0] += 1E-8;
        //         int curIdx = int(msgBuf[0]);
        //         v_msg_bufs[curIdx].push_back(msgBuf[0] - int(msgBuf[0]));
        //         // int curIdx = int(msgBuf[0] + 1E-8);
        //         // v_msg_bufs[curIdx].push_back(msgBuf[0] - int(msgBuf[0]));
        //         curIdx++;
        //         for (int j = 1; j < size; j++) {
        //             msgBuf[j] += 1E-8;
        //             curIdx += int(msgBuf[j]);
        //             v_msg_bufs[curIdx].push_back(msgBuf[j] - int(msgBuf[j]));
        //             // curIdx += int(msgBuf[j] + 1E-8);
        //             // v_msg_bufs[curIdx].push_back(msgBuf[j] - int(msgBuf[j]));
        //             curIdx++;
        //         }
        //     }
        // }
        // gather all messages
        // int curIdx = 0;
        // for (int i = 0; i < np; i++) {
        //     MsgVec& msgBuf = out_messages.getMsgBuf(i);
        //     int size = msgBuf.size();
        //     if (size > 0) {
        //         if (msgBuf[0] - int(msgBuf[0]) > 0.5) {
        //             // msgBuf[j] += 1E-8;
        //             msgBuf[0] = int(msgBuf[0]) + 1;
        //         }
        //         curIdx = int(msgBuf[0]);
        //         v_msg_bufs[curIdx].push_back(msgBuf[0] - int(msgBuf[0]));
        //         // int curIdx = int(msgBuf[0] + 1E-8);
        //         // v_msg_bufs[curIdx].push_back(msgBuf[0] - int(msgBuf[0]));
        //         curIdx++;
        //         for (int j = 1; j < size; j++) {
        //             if (msgBuf[j] - int(msgBuf[j]) > 0.5) {
        //                 // msgBuf[j] += 1E-8;
        //                 msgBuf[j] = int(msgBuf[j]) + 1;
        //             }
        //             curIdx += int(msgBuf[j]);
        //             v_msg_bufs[curIdx].push_back(msgBuf[j] - int(msgBuf[j]));
        //             // curIdx += int(msgBuf[j] + 1E-8);
        //             // v_msg_bufs[curIdx].push_back(msgBuf[j] - int(msgBuf[j]));
        //             curIdx++;
        //         }
        //     }
        // }
        // // gather all messages
        // int curIdx = 0;
        // for (int i = 0; i < np; i++) {
        //     MsgVec& msgBuf = out_messages.getMsgBuf(i);
        //     int size = msgBuf.size();
        //     if (size > 0) {
        //         if (msgBuf[0] - int(msgBuf[0]) > 0.5) {
        //             curIdx = int(msgBuf[0]) + 2;
        //         }
        //         else {
        //             curIdx = int(msgBuf[0]);
        //             v_msg_bufs[curIdx].push_back(msgBuf[0] - int(msgBuf[0]));
        //             curIdx++;
        //         }
        //         for (int j = 1; j < size; j++) {
        //             if (msgBuf[j] - int(msgBuf[j]) > 0.5) {
        //                 curIdx += int(msgBuf[j]) + 2;
        //                 continue;
        //             }
        //             curIdx += int(msgBuf[j]);
        //             v_msg_bufs[curIdx].push_back(msgBuf[j] - int(msgBuf[j]));
        //             // curIdx += int(msgBuf[j] + 1E-8);
        //             // v_msg_bufs[curIdx].push_back(msgBuf[j] - int(msgBuf[j]));
        //             curIdx++;
        //         }
        //     }
        // }

        //clear out-msg-buf
        out_messages.clear();

        return to_add;
    }

    void add_vertex(VertexT* v)
    {
        hasMsg(); //cannot end yet even every vertex halts
        to_add.push_back(v);
    }

    long long get_total_msg()
    {
        return out_messages.get_total_msg();
    }

    int get_total_vadd()
    {
        return to_add.size();
    }

    vector<MessageContainerT>& get_v_msg_bufs()
    {
        return v_msg_bufs;
    }
};

#endif
