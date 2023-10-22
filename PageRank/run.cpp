#include "pregel_app_pagerank.h"

int main(int argc, char* argv[]){
	init_workers();
	int n_round = atoi(argv[1]);
	// double sz_error = stod(argv[2]);

	pregel_pagerank("/toyFolder", "/toyOutput", true, n_round);
	// pregel_pagerank("/toyFolder", "/toyOutput", true, n_round, sz_error);

	worker_finalize();
	return 0;
}
