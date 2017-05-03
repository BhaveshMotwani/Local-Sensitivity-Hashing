from pyspark import SparkContext
from collections import defaultdict
from operator import add
from itertools import permutations
import sys
sc = SparkContext(appName="inf553i")
gl=1
def main():
    global gl
    f=[]
    mj={}
    d=[];v=[];w=[];x=[];y=[];z=[];o=[];ms=[];kj=[]
    a = sc.textFile(sys.argv[1])
    out=sys.argv[2]
    b=a.flatMap(lambda x:[x.split(',')])
    gl=b.collect()
    d=b.map(lambda x:sign(x))
    k=d.collect()
    for i in k:
	v+=[i[0:4]]
	w+=[i[4:8]]
	x+=[i[8:12]]
	y+=[i[12:16]]
	z+=[i[16:20]]
    mj[1]=v;mj[2]=w;mj[3]=x;mj[4]=y;mj[5]=z
    py=[]
    for s in mj.values():
    	ly=sc.parallelize(s)
    	my=ly.zipWithIndex()
    	ny=my.map(lambda x:(','.join(x[0]),x[1]+1))
    	oy=ny.groupByKey()
    	py+=oy.collect()
    jks=[]
    for i in py:
	if (len(i[1])>1):
		jks.append(i)
    for i in jks:
		ms.append(list(permutations(i[1],2)))
    kj=[val for sub in ms for val in sub]
    kj=list(kj)
    kj=sc.parallelize(kj)
    pq=kj.distinct().groupByKey()
    pq=pq.map(lambda x: (x[0],sorted(list(x[1])))).collect()
    pq.sort(key= lambda x:x[0])
    print(pq[0])
    pq=sc.parallelize(pq)
    qp=pq.map(lambda x:jac(x))
    mons=qp.collect()
    file=open(out,'w')
    for i in mons:
	file.write('U%s:'%i[0])
        for k in range(len(i[1])-1):
		file.write('U%s,'%i[1][k])
	file.write('U%s'%i[1][len(i[1])-1])
	file.write('\n')

def sign(x):
    m=x[1:]
    s=[]
    for i in xrange(0,20):
	b=[]
	for j in range(len(m)):
		b.append((3*int(m[j])+13*i)%100)
	b=sorted(b)
	s.append(str(b[0]))
    return s


def jac(x):
    if(len(x[1])>5):
    	bo=[]
    	a=set(gl[x[0]-1][1:])
    	for i in x[1]:
		b=set(gl[i-1][1:])
		bo.append((float(len(a.intersection(b)))/len(a.union(b)),i))
    	boo=sorted(bo,key=lambda s:s[0],reverse=True)
    	boo=boo[:5]
    	jk=[]
    	for c,d in boo:
		jk.append(d)
    	return (x[0],sorted(jk))
    else:
	return (x[0],x[1])



if __name__ == '__main__':
    main()

