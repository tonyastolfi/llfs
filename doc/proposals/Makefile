DIAGRAMS:=\
./new_page_job_seq.svg \
./page_lifecycle.svg \


.PHONY: all clean
all: $(DIAGRAMS)

clean:
	rm $(DIAGRAMS)

%.svg: %.mmd
	mmdc -i $< -o $@
