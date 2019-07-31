
psort : psort.c helper.c helper.h
	gcc -Wall -g -std=gnu99 -o psort psort.c helper.c

clean : 
	-rm psort helper
