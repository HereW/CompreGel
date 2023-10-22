#include "pregel_push_ppr.h"

int main(int argc, char* argv[]){
	init_workers();
	int n_round = atoi(argv[1]);
	int src = atoi(argv[2]);
	// double sz_error = stod(argv[3]);

	pregel_ppr("/toyFolder", "/toyOutput", true, n_round, src);
	// pregel_ppr("/toyFolder", "/toyOutput", true, n_round, src, sz_error);

	worker_finalize();
	return 0;
}
