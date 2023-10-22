#include "basic/pregel-dev.h"
using namespace std;
double eps = 1E-8;
int src=200;
double alpha=0.2;
int num_round = 10;
struct PPRValue_pregel
{
	double pr;
    double residue;
	double dangling_weights;
	vector<VertexID> edges;
};
ibinstream & operator<<(ibinstream & m, const PPRValue_pregel & v){
	m<<v.pr;
	m<<v.residue;
	m<<v.dangling_weights;
	m<<v.edges;
	return m;
}
obinstream & operator>>(obinstream & m, PPRValue_pregel & v){
	m>>v.pr;
	m>>v.residue;
	m>>v.dangling_weights;
	m>>v.edges;
	return m;
}
class PPRVertex_pregel:public Vertex<VertexID, PPRValue_pregel, double>
{
    public:
        virtual void compute(MessageContainer & messages)
        {
			if(step_num()==1)
			{
				value().pr=0.0;
			}
            else
			{
				double sum=0;
				for(MessageIter it=messages.begin(); it!=messages.end(); it++)
				{
					sum+=*it;
				}
				// double* agg=(double*)getAgg();
				// double residual=*agg;
				value().residue=0;
				value().residue+=sum;
				// value().residue+=residual*value().dangling_weights;
				// value().pr+=alpha*residual*value().dangling_weights;
			}
			
			if(step_num()<num_round){
				value().pr+=alpha*value().residue;
				if (value().residue > 0) {
					double msg=(1-alpha)*value().residue/value().edges.size();
					// value().residue=0;
					for(vector<VertexID>::iterator it=value().edges.begin(); it!=value().edges.end(); it++)
					{
						send_message(*it, msg);
					}
				}
			}
			else vote_to_halt();
        }
};

class PPRAgg_pregel:public Aggregator<PPRVertex_pregel, double, double>
{
	private:
		double sum;
	public:
		virtual void init(){
			sum=0;
		}

		virtual void stepPartial(PPRVertex_pregel* v)
		{
			// if(v->value().edges.size()==0) sum+=v->value().pr;
			if(v->value().edges.size()==0) sum+=v->value().residue;
		}

		virtual void stepFinal(double* part)
		{
			sum+=*part;
		}

		virtual double* finishPartial(){ return &sum; }
		virtual double* finishFinal(){ return &sum; }
};

// class PPRWorker_pregel:public Worker<PPRVertex_pregel>
class PPRWorker_pregel:public Worker<PPRVertex_pregel, PPRAgg_pregel>
{
	char buf[1000];
	public:

		virtual PPRVertex_pregel* toVertex(char* line)
		{
			char * pch;
			pch=strtok(line, "\t");
			PPRVertex_pregel* v=new PPRVertex_pregel;
			int id = atoi(pch);
			v->id=id;
			if(id==src){
				v->value().residue=1.0;
				// v->value().dangling_weights=1.0;
			}
			else{
				v->value().residue=0.0;
				// v->value().dangling_weights=0.0;
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

		virtual void toline(PPRVertex_pregel* v, BufferedWriter & writer)
		{
			sprintf(buf, "%d\t%.14f\n", v->id, v->value().pr);
			writer.write(buf);
		}

		// virtual void toline_res(PPRVertex_pregel* v, BufferedWriter & writer)
		// {
		// 	sprintf(buf, "%d\t%.14f\n", v->id, v->value().residue);
		// 	writer.write(buf);
		// }
};

class PPRCombiner_pregel:public Combiner<double>
{
	public:
		virtual void combine(double & old, const double & new_msg)
		{
			old+=new_msg;
		}
};

void pregel_ppr(string in_path, string out_path, bool use_combiner, int round_in=20, int src_in=200, 
	double sz_error_in=1e-10, double eps_in=1e-8,double alpha_in=0.2){
	WorkerParams param;
	param.input_path=in_path;
	param.output_path=out_path;
	param.force_write=true;
	param.native_dispatcher=false;
	param.sz_error = sz_error_in;
	eps = eps_in;
	alpha = alpha_in;
	num_round = round_in;
	src = src_in;
	PPRWorker_pregel worker;
	PPRCombiner_pregel combiner;
	if(use_combiner) worker.setCombiner(&combiner);
	PPRAgg_pregel agg;
	worker.setAggregator(&agg);
	worker.run(param);
}
