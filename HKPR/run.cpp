#include "pregel_app_hkpr.h"

int main(int argc, char* argv[]){
	init_workers();
	int n_round = atoi(argv[1]);
	int src = atoi(argv[2]);
	int t = atoi(argv[3]);
	// double sz_error = stod(argv[4]);

	pregel_hkpr("/toyFolder", "/toyOutput", true, n_round, src, t);
	// pregel_hkpr("/toyFolder", "/toyOutput", true, n_round, src, t, sz_error);

	worker_finalize();
	return 0;
}
