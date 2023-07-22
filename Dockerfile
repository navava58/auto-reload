FROM ubuntu:lastest

RUN apt-get update
RUN apt-get install vim

WORKDIR /home/anhnn91

ADD anhnn91.txt /home/anhnn91/

ENTRYPOINT ["/bin/bash"]