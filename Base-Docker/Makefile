get-lambda-layer:
	# @echo ___ Starting Lambda Layer creation Utility ____
	# @echo ___ Running Ubuntu container ___
	# docker run -dit --name courier-build ubuntu:latest
	# @echo ___ copying requirements file to container ___
	# docker cp -L ./requirements.txt ubuntu:/root/requirements.in
	# docker exec -it ubuntu bash -c "apt-get update \
	# 	&& apt-get install -y software-properties-common \
	# 	&& add-apt-repository ppa:deadsnakes/ppa \
	# 	&& apt-get install -y  python3.9 python3.9-dev \
	# 	&& apt-get install python3-pip \
	# 	&& apt-get install -y zip \
	# 	&& cd /root/ \
	# 	&& pwd \
	# 	&& mkdir -p build/python/lib/python3.9/site-packages \
	# 	&& python3.9 -m pip install --upgrade pip \
	# 	&& apt-get install -y mysql-server \
	# 	&& apt-get install -y libmysqlclient-dev \
	# 	&& apt-get install -y unixodbc-dev \
	# 	&& apt-get install -y git \
	# 	# && python3.9 -m pip install -r /root/requirements.in -t build/python/lib/python3.9/site-packages"
	# docker exec -it ubuntu bash -c "CLFAGS='-g0 -Wl -I/usr/include:/usr/local/include -L/usr/lib:/usr/local/lib' \
	# 	&& python3.9 -m pip install -r /root/requirements.in --global-option=build_ext"
	# docker exec -it ubuntu bash -c "cd /root/build \
	# 	&& zip -r package.zip ."
	@echo ____ Building container ____
	docker build -t courier-build .
	docker run courier-build
	@echo ____ Zipping python libraries ____
	docker run --mount type=bind,source=$(CURDIR),target=/home/user/host \
	 	-it courier-build \
		bash -c "cd /home/user/.local/lib \
		&& zip -r /home/user/host/package.zip ."
	# @echo ____ Getting python package ____
	# docker cp courier-build:/root/build/package.zip .
	# docker stop courier-build
