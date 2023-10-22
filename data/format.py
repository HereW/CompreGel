# convert COO to adj format in Pregel+ following http://www.cse.cuhk.edu.hk/pregelplus/download.html

import sys
import os
filename = sys.argv[1]

print(filename)

adj = {}
old2new = {}
new2old = {}

vid = 1
vertices = set()

with open(filename, 'r', buffering=1024*1024*1024) as file:
    for line in file:
        if line.startswith('#') or line.startswith('%'):
            continue
        # lst = line.split('\t')
        lst = line.split()
        src = int(lst[0])
        dst = int(lst[1])

        if (src == dst):
            continue
        if src not in old2new:
            old2new[src] = vid
            new2old[vid] = src
            vid += 1
        if dst not in old2new:
            old2new[dst] = vid
            new2old[vid] = dst
            vid += 1
        
        new_src = old2new[src]
        new_dst = old2new[dst]

        assert(new_src != new_dst)

        if new_src not in adj:
            adj[new_src] = []
        adj[new_src].append(new_dst)

print(old2new)
print("# of vertices: {}".format(vid))

for key in adj:
    adj[key].sort()

basename = os.path.basename(filename)
parts = basename.split(".")
write_file=parts[0]+ "-adj.txt"
with open(write_file, 'w') as f:
    for i in range(1, vid):
        f.write(str(i))
        f.write('\t')
        if i not in adj:
            f.write('0\n')
        else:
            f.write(str(len(adj[i])))
            f.write(' ')
            for ne in adj[i]:
                f.write(str(ne))
                f.write(' ')
            f.write('\n')

        
    
