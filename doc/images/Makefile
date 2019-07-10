objects = linear.png linear2.png optionals.png mul_loop.png two_levels.png

all : $(objects)


$(objects): %.png: %.gv
	dot -Tpng $< -o $@

clean :
	rm -f *.png
