#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include "helper.h"
#include <getopt.h>
#include <sys/wait.h>

//Outputs the total entries in the binary file.
int bf_total_entries(FILE *fp){
	int total_entries = 0;
	fseek(fp, 0, SEEK_SET);
	struct rec record_structure;
	while(fread(&record_structure, sizeof(struct rec), 1, fp) > 0){
		total_entries += 1;
	}
	if(total_entries == 0){//If there wasn't anything in the file to 
		//begin with, there is nothing else to do.
		exit(0);
	}
	return total_entries;
}

//Creates a balanced workload for each child.
void create_child_workloads(int num_children, 
	int total_entries, int *records_per_child){
	for(int i = 0; i < num_children; i++){
		records_per_child[i] = total_entries/num_children; 
		//Results in integer division.
	}
	for(int i = 0; i < total_entries % num_children; i++){
		//Distributes the remainder across each child
		records_per_child[i] += 1;
	}
		
}
//returns the fseek value (Where the child should start 
//reading from) given a specified child
int fseek_val(int *records_per_child, int child_num, int num_children){
	if(child_num >= num_children || child_num < 0){
		perror("Invalid child value entry.. ");
		exit(1);	
	}
	int sum = 0;
	for(int i = 0; i < child_num; i++){
		sum+= records_per_child[i];
	}
	return sum;
	
}
//Opens a file safely
FILE* safe_fopen(char * file_pointer, char* mode){
	FILE* output_file = fopen(file_pointer, mode);
	if(output_file == NULL){
		perror("Opening file failed");
		exit(1);
	}
	return output_file;
}
//Closes a file safely
void safe_fclose(FILE * file){
	if(fclose(file) != 0){
		perror("fclose");
		exit(1);	
	}
}
//Safely close a file descriptor
void safe_close(int to_close){
	if(close(to_close) == -1){ 
	//since the child does not read from the pipe
		perror("Closing of read end of child pipe failed");
		exit(to_close);
	}
}

//Safely read without error
void safe_fread(struct rec* r1, int num_elements_to_read, FILE *file){
	if (fread(r1, sizeof(struct rec), num_elements_to_read, file) < 0){
		perror("fread when reading inside a child");
		exit(1);		
	}
}

int main(int argc, char **argv){
	int num_children;
	int c;
	char * inputFile = NULL;
	char * outputFile = NULL;
	if(argc != 7){
		fprintf(stderr, "Usage: psort -n <number of processes>");
		fprintf(stderr, " -f <inputfile> -o <outputfile>\n");
		exit(1);
	}
	//parses the given arguments using getopt
	while((c = getopt(argc, argv, "n:f:o:")) != -1){
		switch(c){
			case 'n':
				num_children = strtol(optarg, NULL, 10);
				break;
			case 'f':
				inputFile = optarg;
				break;
			case 'o':
				outputFile = optarg;
				break;
			default: 
				fprintf(stderr, "Usage: psort -n ");
				fprintf(stderr, "<number of processes>");
				fprintf(stderr, " -f <inputfile> -o ");
				fprintf(stderr, "<outputfile>");
				exit(1);
		}
	}
	if(num_children <= 0){
		fprintf(stderr, "You must allow for at least one child for \
			this process to complete! \
			You allowed for %d child\n", num_children);
		exit(1);
	}	
	FILE* binary_file = safe_fopen(inputFile, "rb");
	int total_entries = bf_total_entries(binary_file);
	safe_fclose(binary_file);
	int records_per_child[num_children];
	create_child_workloads(num_children, total_entries, 
	records_per_child);
	int pipe_registry[num_children][2];
	//creates a pipe at the specified index
	for(int i = 0; i < num_children; i++){
		if (pipe(pipe_registry[i]) == -1){
			perror("pipe");
			exit(1);	
		}
		int result = fork();
		if (result < 0){
			fprintf(stderr, "child %d failed\n", i);
			perror("Forking of child failed\n");
			exit(1);	
		}
		else if(result == 0){//Not parent
			//Read in the correct elements, Call the helper 
			//function to sort the stuff, write to the pipe.
			safe_close(pipe_registry[i][0]);
			FILE* binary_file = safe_fopen(inputFile, "rb");
			int start_index = fseek_val(records_per_child,
			 i, num_children);
			int end_index = start_index + records_per_child[i] - 1;
			//end_index: last valid index that the 
			//child should read from file.
			int num_elements_to_read = end_index - start_index + 1;
			struct rec r1[num_elements_to_read];
			if(fseek(binary_file, start_index * sizeof(struct rec),
			SEEK_SET) != 0){
				perror("Fseek failed");
				exit(i+1);			
			}
			//Populate array with correct elements
			safe_fread(r1, num_elements_to_read, binary_file);
			//sort array in place
			qsort(r1, num_elements_to_read, sizeof(struct rec),
			compare_freq);
			//Writes elements one at a time to the pipe
			for(int j = 0; j < num_elements_to_read; j++){
				if(write(pipe_registry[i][1], &(r1[j]), 
				sizeof(struct rec)) != sizeof(struct rec)){
					perror("Writing from child \
						to pipe failed");
					exit(i+1);
				}
			}
			safe_close(pipe_registry[i][1]);
			safe_fclose(binary_file);
			exit(0); //Don't want to fork more children	
		}
		else{
			safe_close(pipe_registry[i][1]); 
			//Since the parent does not write to the pipe
		}
	}
	FILE *output_file = safe_fopen(outputFile, "wb");	
	struct rec one_per_child[num_children];
	struct rec neg_struct;
	neg_struct.freq = -1;
	//A filler struct.
	for(int k = 0; k < num_children; k++){
		if (read(pipe_registry[k][0], &(one_per_child[k]),
			sizeof(struct rec)) == 0){
			//if that particular child did no work.
			one_per_child[k] = neg_struct;
		}	
	}
	//Writes exactly the number of entries in the old file to the new one
	for(int written_counter = 0; written_counter < total_entries; 
		written_counter++){
		int smallest_index = 0;
		int index = 0;
		while(one_per_child[index].freq < 0){
		//Find an initial value with which to compare to; 
		//guarenteed to find at least one else error.
			index++;		
		}
		int smallest_freq = one_per_child[index].freq;
		smallest_index = index;

		for(int m = 0; m < num_children; m++){
		//Find the smallest value, which should be 
		//written to the output file
			if(one_per_child[m].freq != -1 && 
				one_per_child[m].freq < smallest_freq){
				smallest_index = m;
				smallest_freq = one_per_child[m].freq;
			}
		}
		//printf("Printing smallest freq to output... %d\n", 
			//one_per_child[smallest_index].freq);
		if (fwrite(&(one_per_child[smallest_index]), 
			sizeof(struct rec), 1, output_file) != 1){
		//Writing a value to the file
			perror("Couldnt write properly to the output file");
			exit(written_counter);	
		}
		if (read(pipe_registry[smallest_index][0], 
		&(one_per_child[smallest_index]), sizeof(struct rec)) == 0){
			//If parent finishes any child, then dummy struct
			one_per_child[smallest_index] = neg_struct;
		}
	}
	int status;
	int pid;
	//Wait for children to finish; then parent finishes
	for (int b = 0; b < num_children; b++){
		if((pid = wait(&status)) == -1){
			perror("wait");
			exit(b+1);		
		}
		if (!WIFEXITED(status)) {
			fprintf(stderr, "Child terminated abnormally\n");
		} else if(WIFSIGNALED(status)){
		        fprintf(stderr, "Child terminated abnormally\n");
		}
			
	}
	//Finally, close all the read ends of parent
	for (int g = 0; g < num_children; g++){
		safe_close(pipe_registry[g][0]);
	}
	safe_fclose(output_file);


	return 0;
}
