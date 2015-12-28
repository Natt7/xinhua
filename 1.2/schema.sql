create database db;
use db;

create schema jt_x_xsdmx_schema
source type csv
fields (
	 xsdmxid       		type string,
	 xsdbdmxid     		type string,
	 xsdid         		type string,
	 bmspkfmxid    		type string,
	 bjqdh     	  		type string,
	 zddm     	  		type string,
	 spxxid    	  		type string,
	 xsfsid        		type string,
	 dj     		  	type double,
	 xz     		  	type double,
	 xj     		  	type double,
	 jj     		  	type double,
	 jz     		  	type double,
	 gysid     	  		type string,
	 sl     		  	type double,
	 fhsl     	  		type double,
	 fhsy     	  		type double,
	 fhmy     	  		type double,
	 sdsl     	  		type double,
	 sdsy     	  		type double,
	 sdmy     	  		type double,
	 zzsl     	  		type double,
	 zzsy     	  		type double,
	 zzmy     	  		type double,
	 yjsl     	  		type double,
	 yjsy     	  		type double,
	 yjmy     	  		type double,
	 bz     		  	type string,
	 pfdmxid       		type string,
	 pfdid      	  	type string,
	 khddmxid      		type string,
	 khddid        		type string,
	 xtsl     	  		type double,
	 xtsy     	  		type double,
	 xtmy     	  		type double,
	 pxdmxid       		type string,
	 pxdid      	  	type string,
	 posid      	  	type string,
	 cghth      	  	type string,
	 ysdh     	  		type string,
	 wxtcgdid      		type string,
	 wxtcgdzpid    		type string,
	 wxtcgdmxid    		type string,
	 wlmxid     	  	type string,
	 wlzdid     	  	type string,
	 dkkfyid       		type string,
	 kfssbmid      		type string,
	 lxtmxid       		type string,
	 ydsl     	  		type u64,
	 djrq     	  		type datetime format "%Y-%m-%d",
	 xsrq     	  		type datetime format "%Y-%m-%d",
	 wlzpdh     	  	type string,
	 wlzpmx     	  	type string,
	 khspmxid      		type string,
	 cylx          		type string
)
record delimiter "lf" 
field delimiter "," 
text qualifier "dqm";
create parser jt_x_xsdmx_parser
type rcd
schema jt_x_xsdmx_schema;
create table jt_x_xsdmx using jt_x_xsdmx_parser;
create index jt_x_xsdmx_index on table jt_x_xsdmx(xsdmxid);


create schema jt_x_xsd_schema
source type csv
fields (
	   xsdid             type string,
	   xsdh              type string,
	   xsywlxid          type string,
	   xslx              type string,
	   xsbmid            type string,
	   wlbmid            type string,
	   khid              type string,
	   shdwid            type string,
	   shdzid            type string,
	   fhydh             type string,
	   wlfhdh            type string,
	   sl                type u64,
	   ysfsid            type string,
	   jsfsid            type string,
	   djrq              type datetime format "%Y-%m-%d",
	   xsrq              type datetime format "%Y-%m-%d",
	   czrq              type datetime format "%Y-%m-%d",
	   czyid             type string,
	   fhpzs             type double,
	   fhzsl             type double,
	   fhzsy             type double,
	   fhzmy             type double,
	   fhbjs             type double,
	   sdpzs             type double,
	   sdzsl             type double,
	   sdzsy             type double,
	   sdzmy             type double,
	   sdbjs             type double,
	   zzzsl             type double,
	   zzzsy             type double,
	   zzzmy             type double,
	   yjzsl             type double,
	   yjzsy             type double,
	   yjzmy             type double,
	   qsjsrq            type datetime format "%Y-%m-%d",
	   zt                type string,
	   yzt               type string,
	   bz                type string,
	   ztid              type string,
	   pzh               type string,
	   jjfh              type string,
	   sjfhrq            type datetime format "%Y-%m-%d",
	   lxtzh             type string,
	   pzztflag          type string,
	   jsztflag          type string,
	   xtfprq            type datetime format "%Y-%m-%d",
	   xtwxsd            type string,
	   b2bqrrq           type datetime format "%Y-%m-%d",
	   b2bsdzsl          type u64,
	   hsbmid            type string,
	   dbrq              type datetime format "%Y-%m-%d",
	   senddate          type datetime format "%Y-%m-%d",
	   hkjzrq            type datetime format "%Y-%m-%d",
	   tjsqdh            type string,
	   pfdid             type string,
	   pfzjdid           type string,
	   zpfh              type string,
	   wlzpdh            type string,
	   xfrq              type datetime format "%Y-%m-%d",
	   mdjsrq            type datetime format "%Y-%m-%d",
	   ykbz              type string,
	   gdbz              type string,
	   dycs              type double,
	   bhsxssy           type double,
	   jjzmy             type double,
	   se                type double,
	   yf                type double,
	   hygsid            type string,
	   dzflag            type string,
	   dzzsy             type double,
	   dzzcy             type double,
	   skzrr             type string,
	   flzje             type double,
	   dzr               type string,
	   dzrq              type datetime format "%Y-%m-%d",
	   ywlxid            type string,
	   jhzcb             type double,
	   hwjs              type u64,
	   khshr             type string,
	   khdh              type string,
	   ysbh              type string,
	   cwsdbz            type string,
	   cwsdr             type string,
	   cwsdrq            type datetime format "%Y-%m-%d"
)
record delimiter "lf" 
field delimiter "," 
text qualifier "dqm";
create parser jt_x_xsd_parser
type rcd
schema jt_x_xsd_schema;
create table jt_x_xsd using jt_x_xsd_parser;
create index jt_x_xsd_index on table jt_x_xsd(xsdid);

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

create schema jt_j_dqbm_schema
source type csv
fields (
	 dqid     			type string,
	 dqbh      			type string,
	 dqmc      			type string,
	 dqjc      			type string,
	 zjm      			type string,
	 sfid      			type string,
	 zt      			type string,
	 cjr      			type string,
	 tyr      			type string,
	 czrq      			type string,
	 sjdqid             type string,
	 dqjs               type string
)
record delimiter "lf" 
field delimiter "," 
text qualifier "dqm";
create parser jt_j_dqbm_parser
type rcd
schema jt_j_dqbm_schema;
create table jt_j_dqbm using jt_j_dqbm_parser;
create index jt_j_dqbm_index on table jt_j_dqbm(dqid);

create schema 1_2_result_schema
source type csv
fields (
	 xsdh     			type string,
	 khid      			type string,
	 dwmc      			type string,
	 ykbz      			type string,
	 zpfh      			type string,
	 cwdlid      		type string,
	 cwfl      			type string,
	 rjfxid      		type string,
	 rjflmc      		type datetime format "%Y-%m-%d",
	 sdsl      			type double,
	 sdmy      			type double,
	 sdsy      			type double,
	 mdjsrq      		type datetime format "%Y-%m-%d",
	 dqid				type string,
	 dqmc				type string
)
record delimiter "lf" 
field delimiter "," 
text qualifier "dqm";
create parser 1_2_result_parser
type rcd
schema 1_2_result_schema;
create table 1_2_result using 1_2_result_parser;
create index 1_2_result_index on table 1_2_result(xsdh);
create statistics model 1_2_result_sum on table 1_2_result
group by ("xsdh","khid","dwmc","cwdlid","cwfl","rjfxid","rjflmc","mdjsrq","dqid","dqmc")
measures (sum(sdsl),sum(sdmy),sum(sdsy));
