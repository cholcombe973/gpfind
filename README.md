# gpfind
Gluster Gfapi parallel find

Use this to find and print files on a gluster filesystem.  It utilizes a threadpool and gfapi bindings to list files much 
faster than regular find.  

This is roughly equiv to: `find /mnt/glusterfs/ -type f | xargs -n1 -P5 ls`
