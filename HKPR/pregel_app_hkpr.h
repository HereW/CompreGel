#include "basic/pregel-dev.h"
#include <cmath>
using namespace std;
//double alpha=0.2;
// double t=5;
double t=10;
double eps=1e-8;
int src=200;
int num_round = 10;
struct HKPRValue_pregel
{
	double hkpr;
    double residue;
	double w_i;
	double Y_i;
	vector<VertexID> edges;
};
ibinstream & operator<<(ibinstream & m, const HKPRValue_pregel & v){
	m<<v.hkpr;
	m<<v.residue;
	m<<v.w_i;
	m<<v.Y_i;
	m<<v.edges;
	return m;
}

obinstream & operator>>(obinstream & m, HKPRValue_pregel & v){
	m>>v.hkpr;
	m>>v.residue;
	m>>v.w_i;
	m>>v.Y_i;
	m>>v.edges;
	return m;
}
class HKPRVertex_pregel:public Vertex<VertexID, HKPRValue_pregel, double>
{
	public:
		virtual void compute(MessageContainer & messages)
		{
			if(step_num()==1)
			{
				value().hkpr=0.0;
				value().Y_i=1.0;
				value().w_i=1.0*exp((-1)*t);

			}
			else
			{
				double sum=0;
				for(MessageIter it=messages.begin(); it!=messages.end(); it++)
				{
					sum+=*it;
				}
				//double* agg=(double*)getAgg();
				//double residual=*agg/get_vnum();
				value().residue=0;
				value().residue+=sum;
			}
			if(step_num()<num_round){
				value().hkpr+=(value().w_i/value().Y_i)*value().residue;
				if (value().residue>0) {
					double msg=(1-value().w_i/value().Y_i)*value().residue/value().edges.size();
					for(vector<VertexID>::iterator it=value().edges.begin(); it!=value().edges.end(); it++)
					{
						send_message(*it, msg);
					}
				}
				value().Y_i-=value().w_i;
				// value().w_i*=t/(step_num()+1);
				value().w_i*=t/step_num();
			}
			else vote_to_halt();
		}

};
class HKPRWorker_pregel:public Worker<HKPRVertex_pregel>
{
	char buf[1000];
	public:

		virtual HKPRVertex_pregel* toVertex(char* line)
		{
			char * pch;
			pch=strtok(line, "\t");
			HKPRVertex_pregel* v=new HKPRVertex_pregel;
			int id = atoi(pch);
			v->id=id;
			if(id==src){
				v->value().residue=1.0;
			}
			else{
				v->value().residue=0.0;
			}
			pch=strtok(NULL, " ");
			int num=atoi(pch);
			for(int i=0; i<num; i++)
			{
				pch=strtok(NULL, " ");
				v->value().edges.push_back(atoi(pch));
			}
			return v;
		}

		virtual void toline(HKPRVertex_pregel* v, BufferedWriter & writer)
		{
			sprintf(buf, "%d\t%.14f\n", v->id, v->value().hkpr);
			writer.write(buf);
		}
};
class HKPRCombiner_pregel:public Combiner<double>
{
	public:
		virtual void combine(double & old, const double & new_msg)
		{
			old+=new_msg;
		}
};

void pregel_hkpr(string in_path, string out_path, bool use_combiner, int round_in=20, int src_in=200, 
	double t_in=10, double sz_error_in=1e-10, double eps_in=1e-8){
	WorkerParams param;
	param.input_path=in_path;
	param.output_path=out_path;
	param.force_write=true;
	param.native_dispatcher=false;
	param.sz_error = sz_error_in;
	eps = eps_in;
	t = t_in;
	num_round = round_in;
	src = src_in;
	HKPRWorker_pregel worker;
	HKPRCombiner_pregel combiner;
	if(use_combiner) worker.setCombiner(&combiner);
	worker.run(param);
}