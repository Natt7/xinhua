create database db;
use db;

create schema jt_c_bmspkfmx_schema
source type csv
fields (
	 bmspkfmxid         	type string,
	 bmspkftzid         	type string,
	 ywbmid         		type string,
	 wlbmid         		type string,
	 spxxid         		type string,
	 gysid             		type string,
	 yxfsid             	type string,
	 dj                 	type double,
	 jz                 	type double,
	 jj                 	type double,
	 xz                 	type double,
	 xj                 	type double,
	 qckc                  	type double,
	 qcmy                  	type double,
	 qcsy                  	type double,
	 jhsl                  	type double,
	 jhmy                  	type double,
	 jhsy                  	type double,
	 xtsl                  	type double,
	 xtmy                  	type double,
	 xtsy                  	type double,
	 drsl                  	type double,
	 drmy                  	type double,
	 drsy                  	type double,
	 xssl                  	type double,
	 xsmy                  	type double,
	 xssy                  	type double,
	 jtsl                  	type double,
	 jtmy                  	type double,
	 jtsy                  	type double,
	 dcsl                  	type double,
	 dcmy                  	type double,
	 dcsy                  	type double,
	 bfsl                  	type double,
	 bfmy                  	type double,
	 bfsy                  	type double,
	 sysl                  	type double,
	 symy                  	type double,
	 sysy                  	type double,
	 qmkc                  	type double,
	 qmmy                  	type double,
	 qmsy                  	type double,
	 jzrq                  	type datetime format "%Y-%m-%d",
	 zt                     type string,
	 yzt                 	type string,
	 bz                 	type string,
	 pfsl                   type double,
	 pfmy                   type double,
	 pfsy                   type double,
	 cgshmxid            	type string,
	 ztid  					type string,
	 ytsl  					type double,
	 ytsy  					type double,
	 ytmy  					type double,
	 ztbz  					type string,
	 zq                     type datetime format "%Y-%m-%d",
	 jxsl                   type double,
	 xsztsl                 type double,
	 jtztsl                 type double,
	 tmsl                   type double,
	 xsztsy                 type double,
	 xsztmy                 type double,
	 jtztsy                 type double,
	 jtztmy                 type double,
	 ktbz    				type string,
	 shdid                	type string,
	 tzscrq            		type datetime format "%Y-%m-%d",
	 yksl   				type u64,
	 bbid   				type string,
	 bcsc   				type string,
	 nbztsl                 type double,
	 nbztsy                 type double,
	 nbztmy                 type double,
	 djsl                   type u64,
	 cssl                   type u64,
	 cssy                   type double,
	 csmy                   type double,
	 cszysl                 type double,
	 cszysy                 type double,
	 cszymy                 type double,
	 csdclzysl              type double,
	 csdclzysy              type double,
	 csdclzymy              type double
)
record delimiter "lf" 
field delimiter "," 
text qualifier "dqm";
create parser jt_c_bmspkfmx_parser
type rcd
schema jt_c_bmspkfmx_schema;
create table jt_c_bmspkfmx using jt_c_bmspkfmx_parser;
create index jt_c_bmspkfmx_index on table jt_c_bmspkfmx(bmspkfmxid);

create schema jt_c_khspmx_schema
source type csv
fields (
	 khspmxid                 		type string,
	 khsptzid                 		type string,
	 ywbmid                       	type string,
	 khid                     		type string,
	 bmspkfmxid                   	type string,
	 spxxid                  		type string,
	 dj                             type double,
	 jz                             type double,
	 jj                             type double,
	 qckc                           type double,
	 qcmy                           type double,
	 qcsy                           type double,
	 jhsl                           type double,
	 jhmy                           type double,
	 jhsy                           type double,
	 xssl                           type double,
	 xsmy                           type double,
	 xssy                           type double,
	 jtsl                           type double,
	 jtmy                           type double,
	 jtsy                           type double,
	 qmkc                           type double,
	 qmmy                           type double,
	 qmsy                           type double,
	 jzrq                           type datetime format "%Y-%m-%d",
	 xsfsid                       	type string,
	 thcs                        	type u64,
	 zt                        		type string,
	 yzt                       		type string,
	 bz                        		type string,
	 yjsl                        	type double,
	 yjsy                        	type double,
	 yjmy                        	type double,
	 ytcs                        	type double,
	 ytmy                        	type double,
	 ytsy                        	type double,
	 xsdmxid                		type string,
	 zzsl                    		type double,
	 zzsy                    		type double,
	 zzmy                    		type double,
	 ztid                    		type string,
	 xsztsl                         type u64,
	 xtztsl                         type u64,
	 cbzk                           type double,
	 cbjg                           type double,
	 xsztsy                     	type double,
	 xsztmy                     	type double,
	 xtztsy                     	type double,
	 xtztmy                     	type double,
	 gysid                      	type string,
	 wlzdid                      	type string,
	 wlmxid                      	type string,
	 spkt         					type string,
	 zqclflag   					type string,
	 xtsl                           type double,
	 xtmy                           type double,
	 xtsy                           type double,
	 bssl                           type double,
	 bsmy                           type double,
	 bssy                           type double,
	 bysl                           type double,
	 bymy                           type double,
	 bysy                           type double,
	 bfsl                           type double,
	 bfmy                           type double,
	 bfsy                           type double
)
record delimiter "lf" 
field delimiter "," 
text qualifier "dqm";
create parser jt_c_khspmx_parser
type rcd
schema jt_c_khspmx_schema;
create table jt_c_khspmx using jt_c_khspmx_parser;
create index jt_c_khspmx_index on table jt_c_khspmx(khspmxid);


create schema jt_j_spxx_schema
source type csv
fields (
      spxxid              type string,
      spbh                type string,
      txm                 type string,
      pm                  type string,
      dj                  type double,
      fxflid              type string,
      cbflid              type string,
      cbsid               type string,
      bbid                type string,
      zz                  type string,
      bz                  type string,
      yz                  type string,
      kbid                type string,
      zzid                type string,
      zl                  type double,
      hd                  type double,
      sl                  type double,
      cbny                type string,
      bc                  type string,
      yzid                type string,
      tsid                type string,
      gg                  type string,
      xh                  type string,
      yc                  type double,
      yssj                type string,
      yzs                 type double,
      lrry                type string,
      lrrq                type string,
      tyry                type string,
      tyrq                type string,
      zt                  type string,
      beiz                type string,
      ywlx                type string,
      yxjz                type string,
      yxbz                type string,
      yxdvd               type string,
      yxjxsl              type double,
      yxxxsl              type double,
      yxyz                type string,
      yxpc                type string,
      yxdy                type string,
      yxzyyy              type string,
      csm                 type string,
      ym                  type double,
      spsxid              type string,
      xgrq                type string,
      isbn                type string,
      yxfxs               type string,
      yxjxs               type string,
      xgr                 type string,
      cwdlid              type string,
      xtxm                type string,
      ttmbz               type string,
      gjid                type string,
      spxxkey             type string,
      spsys               type double,
      xtnzy               type string,
      xsbz                type string,
      shuji               type string,
      spsm                type string,
      bwb                 type string,
      zs                  type double,
      gjz                 type string,
      czrq                type string,
      ttmbz2              type string,
      jcjf                type string,
      lydwid              type string,
      ysid                type string,
      cd                  type string,
      bzq                 type double,
      yymarc              type string,
      qh                  type string,
      kh                  type string,
      yfdh                type string,
      mlj                 type double,
      gwkj                type double,
      whpj                type double,
      newsku              type string,
      mbts                type u64,
      mtcs                type u64,
      ztflag              type string,
      xsspflag            type string,
      ptpflag             type string,
      fspflag             type string,
      zspflag             type string,
      zddmxid             type string,
      sbid                type string,
      nrjj                type string,
      fmtp                type string,
      zddm                type string,
	  splbid              type string,
      splxid              type string
)
record delimiter "lf" 
field delimiter "," 
text qualifier "dqm";
create parser jt_j_spxx_parser
type rcd
schema jt_j_spxx_schema;
create table jt_j_spxx using jt_j_spxx_parser;
create index jt_j_spxx_index on table jt_j_spxx(spxxid);

create schema jt_j_fxfl_rjfl_schema
source type csv
fields (
	fxflid           type string,
	fxflmc           type string,
	rjfxid           type string,
	rjflmc           type string,
	cwdlid           type string
)
record delimiter "lf" 
field delimiter "," 
text qualifier "dqm";
create parser jt_j_fxfl_rjfl_parser
type rcd
schema jt_j_fxfl_rjfl_schema;
create table jt_j_fxfl_rjfl using jt_j_fxfl_rjfl_parser;
create index jt_j_fxfl_rjfl_index on table jt_j_fxfl_rjfl(fxflid); 

create schema jt_j_cwdl_schema
source type csv
fields (
	 cwdlid     		type string,
	 flbh      			type string,
	 cwfl      			type string,
	 fljc      			type string,
	 zjm      			type string,
	 zt      			type string,
	 cjr      			type string,
	 tyr      			type string,
	 czrq      			type datetime format "%Y-%m-%d",
	 sl      			type u64
)
record delimiter "lf" 
field delimiter "," 
text qualifier "dqm";
create parser jt_j_cwdl_parser
type rcd
schema jt_j_cwdl_schema;
create table jt_j_cwdl using jt_j_cwdl_parser;
create index jt_j_cwdl_index on table jt_j_cwdl(cwdlid);

create schema jt_j_dwxx_schema
source type csv
fields (
	dwid           type string,
	dwbh           type string,
	dwmc           type string,
	dwjc           type string,
	zjm            type string,
	sjdwid         type string,
	dwsxid         type string,
	bmlxid         type string,
	gyslxid        type string,
	khlxid         type string,
	kflxid         type string,
	yinsdwlxid     type string,
	cbslxid        type string,
	ysdwlxid       type string,
	bmztid         type string,
	gzlid          type string,
	dwjb           type u64,
	dwfr           type string,
	sfid           type string,
	dqid           type string,
	yzbm           type string,
	txdz           type string,
	dh             type string,
	cz             type string,
	lxr            type string,
	khyh           type string,
	zh             type string,
	sh             type string,
	email          type string,
	wz             type string,
	jsfsid         type string,
	tssx           type double,
	tsxx           type double,
	sdsx           type double,
	sdxx           type double,
	zksx           type double,
	zkxx           type double,
	jsdwid         type string,
	djsdbz         type string,
	tsjsdbz        type string,
	khzzyfbz       type double,
	khzzbzfbz      type double,
	khzzqtfybz     type double,
	cgjszq         type u64,
	tsjszq         type u64,
	ysfsid         type string,
	zzdbz          type string,
	yzdbh          type string,
	ezdbh          type string,
	tdyzs          type u64,
	dwtjbh         type string,
	cgjsyxjb       type u64,
	xsjsyxjb       type u64,
	phyxjb         type u64,
	xtyxjb         type u64,
	jtyxjb         type u64,
	cgyxjb         type u64,
	zt             type string,
	cjr            type string,
	tyr            type string,
	czrq           type string,
	bz             type string,
	mrxszk         type double,
	mrxsfsid       type string,
	szwzid         type string,
	zdkhfhzk       type double,
	gljy           type string,
	laberflag      type string,
	thttbl         type u64,
	cght           type string,
	sjxz           type string,
	cgfpye         type double,
	gyshzdjid      type string,
	djdyxs         type string,
	jsdwmc         type string,
	kpsx           type string,
	dwxxkey        type string,
	yxzf           type string,
	yxbb           type string,
	fxfsid         type string,
	zzjgdm         type string,
	yyzz           type string
)
record delimiter "lf" 
field delimiter "," 
text qualifier "dqm";
create parser jt_j_dwxx_parser
type rcd
schema jt_j_dwxx_schema;
create table jt_j_dwxx using jt_j_dwxx_parser;
create index jt_j_dwxx_index on table jt_j_dwxx(dwid);

create schema 3_2_result_schema
source type csv
fields (
	 khid     			type string,
	 dwmc     			type string,
	 cwdlid     		type string,
	 cwfl      			type string,
	 rjfxid      		type string,
	 rjflmc      		type string,
	 qmmy      			type double,
	 jzrq      			type datetime format "%Y-%m-%d"
)
record delimiter "lf" 
field delimiter "," 
text qualifier "dqm";
create parser 3_2_result_parser
type rcd
schema 3_2_result_schema;
create table 3_2_result using 3_2_result_parser;
create index 3_2_result_index on table 3_2_result(cwdlid);
create statistics model 3_2_result_sum on table 3_2_result
group by ("khid","dwmc","cwdlid","cwfl","rjfxid","rjflmc","jzrq")
measures (sum(qmmy));
