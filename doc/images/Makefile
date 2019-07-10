objects = linear.png linear2.png optionals.png

all : $(objects)


$(objects): %.png: %.gv
	dot -Tpng $< -o $@

clean :
	rm -f *.png
