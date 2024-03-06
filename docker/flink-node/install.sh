#!/bin/bash
set -e
set -u

apt-get update
apt-get install -y build-essential
#
###########
wget http://www.ijg.org/files/jpegsrc.v8c.tar.gz
tar -xf jpegsrc.v8c.tar.gz
rm  jpegsrc.v8c.tar.gz
cd jpeg-8c
./configure
make
make install
cd ..
rm -rf jpeg-8c

#ghostscript
wget https://github.com/ArtifexSoftware/ghostpdl-downloads/releases/download/gs923/ghostscript-9.23.tar.xz
tar -xf ghostscript-9.23.tar.xz
rm ghostscript-9.23.tar.xz
cd ghostscript-9.23
./configure
make
make install
cd ..
rm -rf ghostscript-9.23

#libtiff
wget http://download.osgeo.org/libtiff/tiff-4.0.6.tar.gz
tar -xf tiff-4.0.6.tar.gz
rm  tiff-4.0.6.tar.gz
cd tiff-4.0.6
./configure --disable-static
make
make install
cd ..
rm -rf tiff-4.0.6
cd /

#image magic
apt-get install -y imagemagick

#Installing ffprobe
wget https://www.johnvansickle.com/ffmpeg/old-releases/ffmpeg-4.2.2-amd64-static.tar.xz
tar xvf ffmpeg-4.2.2-amd64-static.tar.xz
mv ffmpeg-4.2.2-amd64-static/ffprobe /usr/local/bin/
rm -rf ffmpeg-4.2.2-amd64-static
rm -rf ffmpeg-4.2.2-amd64-static.tar.xz


ranlib /usr/local/lib/libjpeg.a
#to have it configured after restart: create /etc/ld.so.conf.d/imagemagick.conf file (with root) and put /usr/local/lib into it. Then run ldconfig and reboot
ldconfig /usr/local/lib

#clearing apt-get cache and other
apt-get remove -y build-essential
apt autoremove -y
rm -rf /var/lib/apt/lists/*