#! /bin/bash 

if !(test -f /dev/shm/*jpg);
then
	pushd /dev/shm

	wget https://www.pixelstalk.net/wp-content/uploads/2016/03/Double-Cat-Wallpaper.jpg
	wget https://wallpapercave.com/dwp2x/qVnC9UJ.jpg # Retriever, 5:1:1 original 3:1:1:1 2MiB
	wget https://wallpapercave.com/wp/wp11786853.jpg # Owl 6/7 1:1:1:1 					 11MiB
	wget https://wallpapercave.com/wp/wp11177599.jpg # Car 6/7 1:1:1:1					 42MiB
	wget https://wallpapercave.com/wp/wp3878871.jpg  # toyshop 4/7 1:1:1				 6MiB
	wget https://wallpapercave.com/wp/wp11335732.jpg # tiger 6/7 1:1:1					 5MiB
	wget https://wallpapercave.com/uwp/uwp3008684.jpeg # maillot 3/7 1:1:1				 23MiB
	wget https://wallpapercave.com/uwp/uwp2993171.jpeg # Egyption cat 4/7 2:1:1			 69MiB
	wget https://mdl.artvee.com/sftb/104801ab.jpg
	wget https://mdl.artvee.com/sftb/219648fg.jpg
	wget https://mdl.artvee.com/sftb/911093ab.jpg
	wget https://mdl.artvee.com/sftb/400853mt.jpg

	popd
fi
