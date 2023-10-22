//Acknowledgements: this code is implemented by referencing pregel-mpi (https://code.google.com/p/pregel-mpi/) by Chuntao Hong.

#ifndef COMMUNICATION_H
#define COMMUNICATION_H

#include <mpi.h>
#include "time.h"
#include "serialization.h"
#include "global.h"
#include "../SZ3/include/SZ3/api/sz.hpp"

#include <algorithm>
#include <cmath>

//============================================
//Allreduce
int all_sum(int my_copy)
{
    int tmp;
    MPI_Allreduce(&my_copy, &tmp, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
    return tmp;
}

long long master_sum_LL(long long my_copy)
{
    long long tmp = 0;
    MPI_Reduce(&my_copy, &tmp, 1, MPI_LONG_LONG_INT, MPI_SUM, MASTER_RANK, MPI_COMM_WORLD);
    return tmp;
}

long long all_sum_LL(long long my_copy)
{
    long long tmp = 0;
    MPI_Allreduce(&my_copy, &tmp, 1, MPI_LONG_LONG_INT, MPI_SUM, MPI_COMM_WORLD);
    return tmp;
}

char all_bor(char my_copy)
{
    char tmp;
    MPI_Allreduce(&my_copy, &tmp, 1, MPI_BYTE, MPI_BOR, MPI_COMM_WORLD);
    return tmp;
}

/*
bool all_lor(bool my_copy){
	bool tmp;
	MPI_Allreduce(&my_copy, &tmp, 1, MPI_BYTE, MPI_LOR, MPI_COMM_WORLD);
	return tmp;
}
*/

//============================================
//char-level send/recv
void pregel_send(void* buf, int size, int dst)
{
    MPI_Send(buf, size, MPI_CHAR, dst, 0, MPI_COMM_WORLD);
}

void pregel_recv(void* buf, int size, int src)
{
    MPI_Recv(buf, size, MPI_CHAR, src, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
}

//============================================
//binstream-level send/recv
void send_ibinstream(ibinstream& m, int dst)
{
    size_t size = m.size();
    pregel_send(&size, sizeof(size_t), dst);
    pregel_send(m.get_buf(), m.size(), dst);
}

obinstream recv_obinstream(int src)
{
    size_t size;
    pregel_recv(&size, sizeof(size_t), src);
    char* buf = new char[size];
    pregel_recv(buf, size, src);
    return obinstream(buf, size);
}

//============================================
//obj-level send/recv
template <class T>
void send_data(const T& data, int dst)
{
    ibinstream m;
    m << data;
    send_ibinstream(m, dst);
}

template <class T>
T recv_data(int src)
{
    obinstream um = recv_obinstream(src);
    T data;
    um >> data;
    return data;
}

//============================================
//all-to-all
template <class T>
void all_to_all(std::vector<T>& to_exchange)
{
    StartTimer(COMMUNICATION_TIMER);
    //for each to_exchange[i]
    //        send out *to_exchange[i] to i
    //        save received data in *to_exchange[i]
    int np = get_num_workers();
    int me = get_worker_id();
    for (int i = 0; i < np; i++) {
        int partner = (i - me + np) % np;
        if (me != partner) {
            if (me < partner) {
                StartTimer(SERIALIZATION_TIMER);
                //send
                ibinstream m;
                m << to_exchange[partner];
                StopTimer(SERIALIZATION_TIMER);
                StartTimer(TRANSFER_TIMER);
                send_ibinstream(m, partner);
                StopTimer(TRANSFER_TIMER);
                //receive
                StartTimer(TRANSFER_TIMER);
                obinstream um = recv_obinstream(partner);
                StopTimer(TRANSFER_TIMER);
                StartTimer(SERIALIZATION_TIMER);
                um >> to_exchange[partner];
                StopTimer(SERIALIZATION_TIMER);
            } else {
                StartTimer(TRANSFER_TIMER);
                //receive
                obinstream um = recv_obinstream(partner);
                StopTimer(TRANSFER_TIMER);
                StartTimer(SERIALIZATION_TIMER);
                T received;
                um >> received;
                //send
                ibinstream m;
                m << to_exchange[partner];
                StopTimer(SERIALIZATION_TIMER);
                StartTimer(TRANSFER_TIMER);
                send_ibinstream(m, partner);
                StopTimer(TRANSFER_TIMER);
                to_exchange[partner] = received;
            }
        }
    }
    StopTimer(COMMUNICATION_TIMER);
}

template <class T, class T1>
void all_to_all_cat_ori(std::vector<T>& to_exchange1, std::vector<T1>& to_exchange2)
{
    StartTimer(COMMUNICATION_TIMER);
    //for each to_exchange[i]
    //        send out *to_exchange[i] to i
    //        save received data in *to_exchange[i]
    int np = get_num_workers();
    int me = get_worker_id();
    for (int i = 0; i < np; i++) {
        int partner = (i - me + np) % np;
        if (me != partner) {
            if (me < partner) {
                StartTimer(SERIALIZATION_TIMER);
                //send
                ibinstream m;
                m << to_exchange1[partner];
                m << to_exchange2[partner];
                // if (me == np - 2) {
                //     std::cout << "pair size : " << to_exchange1[partner].size() * 12.0 << std::endl;

                // }
                StopTimer(SERIALIZATION_TIMER);
                StartTimer(TRANSFER_TIMER);
                send_ibinstream(m, partner);
                StopTimer(TRANSFER_TIMER);
                //receive
                StartTimer(TRANSFER_TIMER);
                obinstream um = recv_obinstream(partner);
                StopTimer(TRANSFER_TIMER);
                StartTimer(SERIALIZATION_TIMER);
                um >> to_exchange1[partner];
                um >> to_exchange2[partner];
                StopTimer(SERIALIZATION_TIMER);
            } else {
                StartTimer(TRANSFER_TIMER);
                //receive
                obinstream um = recv_obinstream(partner);
                StopTimer(TRANSFER_TIMER);
                StartTimer(SERIALIZATION_TIMER);
                T received1;
                T1 received2;
                um >> received1;
                um >> received2;
                //send
                ibinstream m;
                m << to_exchange1[partner];
                m << to_exchange2[partner];
                StopTimer(SERIALIZATION_TIMER);
                StartTimer(TRANSFER_TIMER);
                send_ibinstream(m, partner);
                StopTimer(TRANSFER_TIMER);
                to_exchange1[partner] = received1;
                to_exchange2[partner] = received2;
            }
        }
    }
    StopTimer(COMMUNICATION_TIMER);
}

template <class T, class T1>
void all_to_all_cat(std::vector<T>& to_exchange1, std::vector<T1>& to_exchange2, double SZerror)
{
    StartTimer(COMMUNICATION_TIMER);
    int np = get_num_workers();
    int me = get_worker_id();

    std::vector<char> compressedDataVec2;
    // compressedDataVec2.reserve(1e6);c

    for (int i = 0; i < np; i++) {
        int partner = (i - me + np) % np;
        // double SZerror=1E-10;
        if (me != partner) {
            if (me < partner) {
                StartTimer(COMPRESSION_TIMER);
                // SZ compression
                // unsigned long outSize = 0;
                // int inSize = to_exchange1[partner].size();
                // char *compressedData;
                // SZ::Config conf(inSize);                     
                // conf.cmprAlgo = SZ::ALGO_INTERP;
                // // conf.cmprAlgo = SZ::ALGO_LORENZO_REG;
                // conf.errorBoundMode = SZ::EB_ABS; // refer to def.hpp for all supported error bound mode
                // conf.absErrorBound = 1E-2; // value-rang-based error bound 1e-3

                unsigned long outSize2 = 0;
                int inSize2 = to_exchange2[partner].size();
                char *compressedData2;
                SZ::Config conf2(inSize2);                     
                conf2.cmprAlgo = SZ::ALGO_INTERP;
                // conf.cmprAlgo = SZ::ALGO_LORENZO_REG;
                conf2.interpAlgo = SZ::INTERP_ALGO_LINEAR;

                conf2.errorBoundMode = SZ::EB_ABS; // refer to def.hpp for all supported error bound mode
                conf2.absErrorBound = SZerror; // value-rang-based error bound 1e-3
                
                //conf2.errorBoundMode = SZ::EB_REL;
                //conf2.relErrorBound = 1E-5;

                //std::vector<char> compressedDataVec;
                // std::vector<char> compressedDataVec2;

                if (inSize2 > 0) {

                    // std::transform(to_exchange1[partner].begin(), to_exchange1[partner].end(), 
                    //         to_exchange1[partner].begin(), [](double x){return std::log(x);});

                    //compressedData = SZ_compress(conf, to_exchange1[partner].data(), outSize);
                    compressedData2 = SZ_compress(conf2, to_exchange2[partner].data(), outSize2);

                    if (me == np - 2) {
                        std::cout << "key size : " << to_exchange1[partner].size() * 4.0 << std::endl;
                        // std::cout << "ori value size : " << to_exchange2[partner].size() * 8.0 << std::endl;
                        //std::cout << "compression ratio: " << inSize * 8.0 / outSize << std::endl;
                        std::cout << "compression ratio : " << inSize2 * 8.0 / outSize2 << std::endl;
                    }
                    // std::cout << "compression ratio : " << inSize2 * 8.0 / outSize2 << std::endl;

                    // std::cout << "n: " << n << std::endl;
                    // std::cout << "outSize: " << outSize << std::endl;
                    // std::cout << "compression ratio: " << inSize * 8.0 / outSize << std::endl;

                    // std::cout << "send inSize: " << inSize << std::endl;
                    // std::cout << "send outSize_i: " << outSize << std::endl;
                    
                    //compressedDataVec.assign(compressedData, compressedData + outSize);
                    compressedDataVec2.assign(compressedData2, compressedData2 + outSize2);
                    // delete[] compressedData2;

                    // if (me == np - 2){
                    //     std::cout << "now value size : " << compressedDataVec2.size() << std::endl;
                    // }
                }
                StopTimer(COMPRESSION_TIMER);
                //send
                StartTimer(SERIALIZATION_TIMER);
                ibinstream m;
                //ibinstream m2;
                // m << inSize;
                // m << int(outSize);
                // m << compressedDataVec;
                m << to_exchange1[partner];
                m << compressedDataVec2;
                m << inSize2;
                m << int(outSize2);
                // m2 << inSize2;
                // m2 << int(outSize2);
                // m2 << compressedDataVec2;
                StopTimer(SERIALIZATION_TIMER);
                
                StartTimer(TRANSFER_TIMER);
                //StartTimer(TRANSFER_KEY_TIMER);
                send_ibinstream(m, partner);
                //StopTimer(TRANSFER_KEY_TIMER);

                // StartTimer(TRANSFER_VAL_TIMER);
                // send_ibinstream(m2, partner);
                // StopTimer(TRANSFER_VAL_TIMER);

                StopTimer(TRANSFER_TIMER);

                //receive
                StartTimer(TRANSFER_TIMER);

                // StartTimer(TRANSFER_VAL_TIMER);
                // obinstream um2 = recv_obinstream(partner);
                // StopTimer(TRANSFER_VAL_TIMER);

                //StartTimer(TRANSFER_KEY_TIMER);
                obinstream um = recv_obinstream(partner);
                //StopTimer(TRANSFER_KEY_TIMER);

                // StartTimer(TRANSFER_VAL_TIMER);
                // obinstream um2 = recv_obinstream(partner);
                // StopTimer(TRANSFER_VAL_TIMER);

                StopTimer(TRANSFER_TIMER);

                StartTimer(SERIALIZATION_TIMER);
                //int outSize_i = 0;
                int outSize_i2 = 0;
                //um >> inSize;
                //um >> outSize_i;
                //um >> compressedDataVec;
                // um2 >> inSize2;
                // um2 >> outSize_i2;
                // um2 >> compressedDataVec2;
                um >> to_exchange1[partner];
                um >> compressedDataVec2;
                um >> inSize2;
                um >> outSize_i2;
                // um2 >> inSize2;
                // um2 >> outSize_i2;
                // um2 >> compressedDataVec2;
                StopTimer(SERIALIZATION_TIMER);

                StartTimer(DECOMPRESSION_TIMER);
                //unsigned long outSize_l = outSize_i;
                unsigned long outSize_l2 = outSize_i2;

                // SZ decompression
                SZ::Config conf_d;   
                // to_exchange1[partner].clear();
                if (inSize2 > 0) {
                    //auto decData = new double[inSize];
                    auto decData2 = new double[inSize2];
                    //SZ_decompress(conf_d, compressedDataVec.data(), outSize_l, decData);
                    SZ_decompress(conf_d, compressedDataVec2.data(), outSize_l2, decData2);

                    //to_exchange1[partner].assign(decData, decData + inSize);
                    to_exchange2[partner].assign(decData2, decData2 + inSize2);
                    // std::transform(to_exchange1[partner].begin(), to_exchange1[partner].end(), 
                    //         to_exchange1[partner].begin(), [](double x){return std::exp(x);});

                    //delete[] decData;
                    delete[] decData2;
                }
                StopTimer(DECOMPRESSION_TIMER);
            } 
            else {
                //receive
                StartTimer(TRANSFER_TIMER);

                // StartTimer(TRANSFER_VAL_TIMER);
                // obinstream um2 = recv_obinstream(partner);
                // StopTimer(TRANSFER_VAL_TIMER);

                //StartTimer(TRANSFER_KEY_TIMER);
                obinstream um = recv_obinstream(partner);
                //StopTimer(TRANSFER_KEY_TIMER);

                // StartTimer(TRANSFER_VAL_TIMER);
                // obinstream um2 = recv_obinstream(partner);
                // StopTimer(TRANSFER_VAL_TIMER);

                StopTimer(TRANSFER_TIMER);

                StartTimer(SERIALIZATION_TIMER);
                // int outSize = 0;
                // int inSize = 0;
                // char *compressedData;
                // std::vector<char> compressedDataVec;
                int outSize2 = 0;
                int inSize2 = 0;
                char *compressedData2;
                // std::vector<char> compressedDataVec2;
                T received1;
                // um2 >> inSize2;
                // um2 >> outSize2;
                // um2 >> compressedDataVec2;
                um >> received1;
                um >> compressedDataVec2;
                um >> inSize2;
                um >> outSize2;
                // T1 received2;
                // um >> inSize;
                // um >> outSize;
                // um >> compressedDataVec;
                // um >> received2;
                // um2 >> inSize2;
                // um2 >> outSize2;
                // um2 >> compressedDataVec2;
                StopTimer(SERIALIZATION_TIMER);

                // std::cout << "(2) received inSize: " << inSize << std::endl;
                // std::cout << "(2) received outSize_i: " << outSize << std::endl;

                // for (int i = 0; i < 10; i++) {
                //     std::cout << "we receive: " << int(compressedData[i]) << std::endl;
                // }
                
                StartTimer(DECOMPRESSION_TIMER);
                SZ::Config conf_d;                     

                //unsigned long outSize_l = outSize;
                unsigned long outSize_l2 = outSize2;
                // SZ decompression
                //auto decData = new double[inSize];
                auto decData2 = new double[inSize2];
                if (inSize2 > 0) {
                    //SZ_decompress(conf_d, compressedDataVec.data(), outSize_l, decData);
                    SZ_decompress(conf_d, compressedDataVec2.data(), outSize_l2, decData2);
                }
                StopTimer(DECOMPRESSION_TIMER);

                StartTimer(COMPRESSION_TIMER);
                //send
                ibinstream m;
                //ibinstream m2;
                // SZ compression
                //unsigned long outSize_temp = 0;
                unsigned long outSize_temp2 = 0;
                //int inSize_temp = to_exchange1[partner].size();
                int inSize_temp2 = to_exchange2[partner].size();

                // std::cout << "inSize_temp: " << inSize_temp << std::endl;

                // SZ::Config conf(inSize_temp);   
                // conf.cmprAlgo = SZ::ALGO_INTERP;
                // // conf.cmprAlgo = SZ::ALGO_LORENZO_REG;
                // conf.errorBoundMode = SZ::EB_ABS; // refer to def.hpp for all supported error bound mode
                // conf.absErrorBound = 1E-2; // value-rang-based error bound 1e-3
                SZ::Config conf2(inSize_temp2);   
                conf2.cmprAlgo = SZ::ALGO_INTERP;
                // conf.cmprAlgo = SZ::ALGO_LORENZO_REG;
                conf2.interpAlgo = SZ::INTERP_ALGO_LINEAR;
                
                conf2.errorBoundMode = SZ::EB_ABS; // refer to def.hpp for all supported error bound mode
                conf2.absErrorBound = SZerror; // value-rang-based error bound 1e-3
                
                //conf2.errorBoundMode = SZ::EB_REL;
                //conf2.relErrorBound = 1E-5;

                if (inSize_temp2 > 0) {
                    // std::transform(to_exchange1[partner].begin(), to_exchange1[partner].end(), 
                    //         to_exchange1[partner].begin(), [](double x){return std::log(x);});

                    //compressedData = SZ_compress(conf, to_exchange1[partner].data(), outSize_temp);
                    compressedData2 = SZ_compress(conf2, to_exchange2[partner].data(), outSize_temp2);
                    // std::cout << "compression ratio: " << inSize_temp * 8.0 / outSize_temp << std::endl;
                }
                StopTimer(COMPRESSION_TIMER);

                StartTimer(SERIALIZATION_TIMER);
                //send
                //m << inSize_temp;
                //m << int(outSize_temp);
                //compressedDataVec.assign(compressedData, compressedData + outSize_temp);
                //m << compressedDataVec;
                // m2 << inSize_temp2;
                // m2 << int(outSize_temp2);
                // compressedDataVec2.assign(compressedData2, compressedData2 + outSize_temp2);
                // m2 << compressedDataVec2;
                m << to_exchange1[partner];
                compressedDataVec2.assign(compressedData2, compressedData2 + outSize_temp2);
                // delete[] compressedData2;
                m << compressedDataVec2;
                m << inSize_temp2;
                m << int(outSize_temp2);
                // m2 << inSize_temp2;
                // m2 << int(outSize_temp2);
                // compressedDataVec2.assign(compressedData2, compressedData2 + outSize_temp2);
                // m2 << compressedDataVec2;

                StopTimer(SERIALIZATION_TIMER);

                StartTimer(TRANSFER_TIMER);

                // StartTimer(TRANSFER_VAL_TIMER);
                // send_ibinstream(m2, partner);
                // StopTimer(TRANSFER_VAL_TIMER);

                //StartTimer(TRANSFER_KEY_TIMER);
                send_ibinstream(m, partner);
                //StopTimer(TRANSFER_KEY_TIMER);

                // StartTimer(TRANSFER_VAL_TIMER);
                // send_ibinstream(m2, partner);
                // StopTimer(TRANSFER_VAL_TIMER);

                StopTimer(TRANSFER_TIMER);

                // to_exchange1[partner].clear();
                if (inSize2 > 0) {
                    //to_exchange1[partner].assign(decData, decData + inSize);
                    to_exchange2[partner].assign(decData2, decData2 + inSize2);
                    // std::transform(to_exchange1[partner].begin(), to_exchange1[partner].end(), 
                    //         to_exchange1[partner].begin(), [](double x){return std::exp(x);});
                    //delete[] decData;
                    delete[] decData2;
                }
                to_exchange1[partner] = received1;
            }
        }
    }
    StopTimer(COMMUNICATION_TIMER);
}

template <class T, class T1, class T2>
void all_to_all_cat(std::vector<T>& to_exchange1, std::vector<T1>& to_exchange2, std::vector<T2>& to_exchange3)
{
    StartTimer(COMMUNICATION_TIMER);
    //for each to_exchange[i]
    //        send out *to_exchange[i] to i
    //        save received data in *to_exchange[i]
    int np = get_num_workers();
    int me = get_worker_id();
    for (int i = 0; i < np; i++) {
        int partner = (i - me + np) % np;
        if (me != partner) {
            if (me < partner) {
                StartTimer(SERIALIZATION_TIMER);
                //send
                ibinstream m;
                m << to_exchange1[partner];
                m << to_exchange2[partner];
                m << to_exchange3[partner];
                StopTimer(SERIALIZATION_TIMER);
                StartTimer(TRANSFER_TIMER);
                send_ibinstream(m, partner);
                StopTimer(TRANSFER_TIMER);
                //receive
                StartTimer(TRANSFER_TIMER);
                obinstream um = recv_obinstream(partner);
                StopTimer(TRANSFER_TIMER);
                StartTimer(SERIALIZATION_TIMER);
                um >> to_exchange1[partner];
                um >> to_exchange2[partner];
                um >> to_exchange3[partner];
                StopTimer(SERIALIZATION_TIMER);
            } else {
                StartTimer(TRANSFER_TIMER);
                //receive
                obinstream um = recv_obinstream(partner);
                StopTimer(TRANSFER_TIMER);
                StartTimer(SERIALIZATION_TIMER);
                T received1;
                T1 received2;
                T2 received3;
                um >> received1;
                um >> received2;
                um >> received3;
                //send
                ibinstream m;
                m << to_exchange1[partner];
                m << to_exchange2[partner];
                m << to_exchange3[partner];
                StopTimer(SERIALIZATION_TIMER);
                StartTimer(TRANSFER_TIMER);
                send_ibinstream(m, partner);
                StopTimer(TRANSFER_TIMER);
                to_exchange1[partner] = received1;
                to_exchange2[partner] = received2;
                to_exchange3[partner] = received3;
            }
        }
    }
    StopTimer(COMMUNICATION_TIMER);
}

template <class T, class T1>
void all_to_all(vector<T>& to_send, vector<T1>& to_get)
{
    StartTimer(COMMUNICATION_TIMER);
    //for each to_exchange[i]
    //        send out *to_exchange[i] to i
    //        save received data in *to_exchange[i]
    int np = get_num_workers();
    int me = get_worker_id();
    for (int i = 0; i < np; i++) {
        int partner = (i - me + np) % np;
        if (me != partner) {
            if (me < partner) {
                StartTimer(SERIALIZATION_TIMER);
                //send
                ibinstream m;
                m << to_send[partner];
                StopTimer(SERIALIZATION_TIMER);
                StartTimer(TRANSFER_TIMER);
                send_ibinstream(m, partner);
                StopTimer(TRANSFER_TIMER);
                //receive
                StartTimer(TRANSFER_TIMER);
                obinstream um = recv_obinstream(partner);
                StopTimer(TRANSFER_TIMER);
                StartTimer(SERIALIZATION_TIMER);
                um >> to_get[partner];
                StopTimer(SERIALIZATION_TIMER);
            } else {
                StartTimer(TRANSFER_TIMER);
                //receive
                obinstream um = recv_obinstream(partner);
                StopTimer(TRANSFER_TIMER);
                StartTimer(SERIALIZATION_TIMER);
                T1 received;
                um >> received;
                //send
                ibinstream m;
                m << to_send[partner];
                StopTimer(SERIALIZATION_TIMER);
                StartTimer(TRANSFER_TIMER);
                send_ibinstream(m, partner);
                StopTimer(TRANSFER_TIMER);
                to_get[partner] = received;
            }
        }
    }
    StopTimer(COMMUNICATION_TIMER);
}

//============================================
//scatter
template <class T>
void masterScatter(vector<T>& to_send)
{ //scatter
    StartTimer(COMMUNICATION_TIMER);
    int* sendcounts = new int[_num_workers];
    int recvcount;
    int* sendoffset = new int[_num_workers];

    ibinstream m;
    StartTimer(SERIALIZATION_TIMER);
    int size = 0;
    for (int i = 0; i < _num_workers; i++) {
        if (i == _my_rank) {
            sendcounts[i] = 0;
        } else {
            m << to_send[i];
            sendcounts[i] = m.size() - size;
            size = m.size();
        }
    }
    StopTimer(SERIALIZATION_TIMER);

    StartTimer(TRANSFER_TIMER);
    MPI_Scatter(sendcounts, 1, MPI_INT, &recvcount, 1, MPI_INT, MASTER_RANK, MPI_COMM_WORLD);
    StopTimer(TRANSFER_TIMER);

    for (int i = 0; i < _num_workers; i++) {
        sendoffset[i] = (i == 0 ? 0 : sendoffset[i - 1] + sendcounts[i - 1]);
    }
    char* sendbuf = m.get_buf(); //ibinstream will delete it
    char* recvbuf;

    StartTimer(TRANSFER_TIMER);
    MPI_Scatterv(sendbuf, sendcounts, sendoffset, MPI_CHAR, recvbuf, recvcount, MPI_CHAR, MASTER_RANK, MPI_COMM_WORLD);
    StopTimer(TRANSFER_TIMER);

    delete[] sendcounts;
    delete[] sendoffset;
    StopTimer(COMMUNICATION_TIMER);
}

template <class T>
void slaveScatter(T& to_get)
{ //scatter
    StartTimer(COMMUNICATION_TIMER);
    int* sendcounts;
    int recvcount;
    int* sendoffset;

    StartTimer(TRANSFER_TIMER);
    MPI_Scatter(sendcounts, 1, MPI_INT, &recvcount, 1, MPI_INT, MASTER_RANK, MPI_COMM_WORLD);

    char* sendbuf;
    char* recvbuf = new char[recvcount]; //obinstream will delete it

    MPI_Scatterv(sendbuf, sendcounts, sendoffset, MPI_CHAR, recvbuf, recvcount, MPI_CHAR, MASTER_RANK, MPI_COMM_WORLD);
    StopTimer(TRANSFER_TIMER);

    StartTimer(SERIALIZATION_TIMER);
    obinstream um(recvbuf, recvcount);
    um >> to_get;
    StopTimer(SERIALIZATION_TIMER);
    StopTimer(COMMUNICATION_TIMER);
}

//================================================================
//gather
template <class T>
void masterGather(vector<T>& to_get)
{ //gather
    StartTimer(COMMUNICATION_TIMER);
    int sendcount = 0;
    int* recvcounts = new int[_num_workers];
    int* recvoffset = new int[_num_workers];

    StartTimer(TRANSFER_TIMER);
    MPI_Gather(&sendcount, 1, MPI_INT, recvcounts, 1, MPI_INT, MASTER_RANK, MPI_COMM_WORLD);
    StopTimer(TRANSFER_TIMER);

    for (int i = 0; i < _num_workers; i++) {
        recvoffset[i] = (i == 0 ? 0 : recvoffset[i - 1] + recvcounts[i - 1]);
    }

    char* sendbuf;
    int recv_tot = recvoffset[_num_workers - 1] + recvcounts[_num_workers - 1];
    char* recvbuf = new char[recv_tot]; //obinstream will delete it

    StartTimer(TRANSFER_TIMER);
    MPI_Gatherv(sendbuf, sendcount, MPI_CHAR, recvbuf, recvcounts, recvoffset, MPI_CHAR, MASTER_RANK, MPI_COMM_WORLD);
    StopTimer(TRANSFER_TIMER);

    StartTimer(SERIALIZATION_TIMER);
    obinstream um(recvbuf, recv_tot);
    for (int i = 0; i < _num_workers; i++) {
        if (i == _my_rank)
            continue;
        um >> to_get[i];
    }
    StopTimer(SERIALIZATION_TIMER);
    delete[] recvcounts;
    delete[] recvoffset;
    StopTimer(COMMUNICATION_TIMER);
}

template <class T>
void slaveGather(T& to_send)
{ //gather
    StartTimer(COMMUNICATION_TIMER);
    int sendcount;
    int* recvcounts;
    int* recvoffset;

    StartTimer(SERIALIZATION_TIMER);
    ibinstream m;
    m << to_send;
    sendcount = m.size();
    StopTimer(SERIALIZATION_TIMER);

    StartTimer(TRANSFER_TIMER);
    MPI_Gather(&sendcount, 1, MPI_INT, recvcounts, 1, MPI_INT, MASTER_RANK, MPI_COMM_WORLD);
    StopTimer(TRANSFER_TIMER);

    char* sendbuf = m.get_buf(); //ibinstream will delete it
    char* recvbuf;

    StartTimer(TRANSFER_TIMER);
    MPI_Gatherv(sendbuf, sendcount, MPI_CHAR, recvbuf, recvcounts, recvoffset, MPI_CHAR, MASTER_RANK, MPI_COMM_WORLD);
    StopTimer(TRANSFER_TIMER);
    StopTimer(COMMUNICATION_TIMER);
}

//================================================================
//bcast
template <class T>
void masterBcast(T& to_send)
{ //broadcast
    StartTimer(COMMUNICATION_TIMER);

    StartTimer(SERIALIZATION_TIMER);
    ibinstream m;
    m << to_send;
    int size = m.size();
    StopTimer(SERIALIZATION_TIMER);

    StartTimer(TRANSFER_TIMER);
    MPI_Bcast(&size, 1, MPI_INT, MASTER_RANK, MPI_COMM_WORLD);

    char* sendbuf = m.get_buf();
    MPI_Bcast(sendbuf, size, MPI_CHAR, MASTER_RANK, MPI_COMM_WORLD);
    StopTimer(TRANSFER_TIMER);

    StopTimer(COMMUNICATION_TIMER);
}

template <class T>
void slaveBcast(T& to_get)
{ //broadcast
    StartTimer(COMMUNICATION_TIMER);

    int size;

    StartTimer(TRANSFER_TIMER);
    MPI_Bcast(&size, 1, MPI_INT, MASTER_RANK, MPI_COMM_WORLD);
    StopTimer(TRANSFER_TIMER);

    StartTimer(TRANSFER_TIMER);
    char* recvbuf = new char[size]; //obinstream will delete it
    MPI_Bcast(recvbuf, size, MPI_CHAR, MASTER_RANK, MPI_COMM_WORLD);
    StopTimer(TRANSFER_TIMER);

    StartTimer(SERIALIZATION_TIMER);
    obinstream um(recvbuf, size);
    um >> to_get;
    StopTimer(SERIALIZATION_TIMER);

    StopTimer(COMMUNICATION_TIMER);
}

#endif
