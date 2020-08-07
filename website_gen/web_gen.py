# -*- coding: utf-8 -*-
"""
Created on Tue Jan 14 12:05:07 2020

@author: Gary
"""
import numpy as np
import pandas as pd
import shutil
import os
import subprocess
from datetime import datetime
import pickle

today = datetime.today()

# for nicer displays of numbers: round to significant figures.
from math import log10, floor
def round_sig(x, sig=2):
    try:
        return int(round(x, sig-int(floor(log10(abs(x))))-1))
    except:
        return x

class Web_gen():
    
    def __init__(self,tab_man=None,data_date='UNKNOWN',
                 caslist = []):
        self.tab_man = tab_man
        self.data_date = data_date
        self.outdir = './tmp/website/'
        self.css_fn = './website_gen/style.css'
        #self.default_empty_fn = './website_gen/default_empty.html'
        self.jupyter_fn = './website_gen/chemical_report.html'
        self.ref_fn = './website_gen/ref.txt'
        self.filtered_fields = ['reckey', 'PercentHFJob', 'record_flags', 
                                'bgMass', 'UploadKey', 'OperatorName',
                                'bgOperatorName',
                                'APINumber', 'TotalBaseWaterVolume',
                                'TotalBaseNonWaterVolume', 'FFVersion', 
                                'TVD', 'StateName', 'StateNumber', 'CountyName', 
                                'CountyNumber', 'TradeName',
                                'Latitude', 'Longitude', 'Projection',
                                'data_source', 'bgStateName', 'bgCountyName', 
                                'bgLatitude', 'bgLongitude', 'date',
                                'IngredientName', 'Supplier', 'bgSupplier', 
                                'Purpose', 'CASNumber', 'bgCAS','infServiceCo',
                                'bgIngredientName', 'proprietary', 
                                'eh_Class_L1', 'eh_Class_L2', 'eh_CAS', 
                                'eh_IngredientName', 'eh_subs_class', 
                                'eh_function','is_on_TEDX']
        self.caslist = caslist
        self.allrec = self.tab_man.get_df_cas(keepcodes='A|M|3',
                                         removecodes='R|1|2|4|5',
                                         event_fields=[])
        self.allrec = self.allrec.filter(self.filtered_fields,axis=1)
        self.allrec['TradeName_trunc'] = np.where(self.allrec.TradeName.str.len()>30,
                                                  self.allrec.TradeName.str[:30]+'...',
                                                  self.allrec.TradeName)
        self.minCount = 5
        # use this to make shortened versions
        if caslist != []:
            self.allrec = self.allrec[self.allrec.bgCAS.isin(caslist)]
        self.num_events = len(self.allrec.UploadKey.unique())

        
    def initialize_dir(self,dir):
        shutil.rmtree(dir,ignore_errors=True)
        os.mkdir(dir)
                      
    def make_dir_structure(self,caslist=[]):
        self.initialize_dir(self.outdir)
        for cas in caslist:
            self.initialize_dir(self.outdir+cas)
        shutil.copyfile(self.css_fn,self.outdir+'style.css')

    def compile_page(self,title='empty title',header='',body=''):
        return f"""<!DOCTYPE html>
<html>
  <head>
    <title>{title}</title>
    <link rel='stylesheet' href='style.css' />
  </head>
  <body>
      <h1>{header}</h1>
      <h4>Data cleaned and extracted from <a href=https://www.FracFocus.org >FracFocus</a>, downloaded on {self.data_date}</h4>
    {body}
  </body>
</html>
"""
    def save_page(self,webtxt='', fn='index.html'):
        with open(self.outdir+fn,'w') as f:
            f.write(webtxt)
    
    def add_table_line(self,vals=[]):
        s = '  <tr>\n'
        for item in vals:        
            s+= f'    <td> {item} </td> \n'
        s+= '   </tr>\n'
        return s
    
    def add_table_head(self,vals=[]):
        s = '  <tr>\n'
        for item in vals:        
            s+= f'    <th> {item} </th> \n'
        s+= '   </tr>\n'
        return s

    def save_global_vals(self,num_UploadKey=None):
        """put numbers used by all analyses into a file for access
        within Jupyter scripts."""
        with open(self.ref_fn,'w') as f:
            f.write(f'{num_UploadKey}\n')
            f.write(f'{self.data_date}\n')
            f.write(f'{today}\n')
        
    def make_chem_list(self):
        t = self.tab_man.tables['cas'].get_df()
        if self.caslist != []:
            t = t[t.bgCAS.isin(self.caslist)]
        t.eh_Class_L1 = np.where(t.eh_Class_L1.isna(),
                                 'not yet classified',
                                 t.eh_Class_L1)
        t.eh_Class_L2 = np.where(t.eh_Class_L2.isna(),
                                 'not yet classified',
                                 t.eh_Class_L2)
        #t = t[t.is_on_TEDX]
        gb = t.groupby('bgCAS',as_index=False)['bgIngredientName',
                                               'eh_Class_L1',
                                               'eh_Class_L2',
                                               'is_on_TEDX'].first()
        #gb = gb[:4]  #limit length for development
        lst = gb.bgCAS.unique().tolist()
        lst.sort()
        self.make_dir_structure(lst)
        
        
        for (i, row) in gb.iterrows():
            chem = row.bgCAS
            print(f'\nWorking on ** {chem} **  ({i})')
            ingred = row.bgIngredientName
            self.save_global_vals(self.num_events)
            
            tt = self.allrec[self.allrec.bgCAS==chem].copy()
            if len(tt)>self.minCount:
                tt.to_csv('website_gen/data.csv',index=False)
                tt.to_csv(self.outdir+chem+'/data.csv',index=False)
                self.make_jupyter_output()
                an_fn = f'/analysis_{chem}.html'
                #print(an_fn)
                shutil.copyfile(self.jupyter_fn,self.outdir+chem+an_fn)
            else:
                print(f'Too few records ({len(tt)}) to do analysis')
                tt.to_csv(self.outdir+chem+'/data.csv',index=False)
                #shutil.copyfile(self.default_empty_fn,self.outdir+chem+an_fn)
    def make_10perc_dict(self,fromScratch=True):
        if fromScratch:
            self.perc90dic = {}
            caslist = self.allrec.bgCAS.unique().tolist()
            for cas in caslist:
                print(cas)
                df = self.allrec[self.allrec.bgCAS==cas][['bgCAS','bgMass']]
                try:
                    perc90_mass = np.percentile(df[df.bgMass>0].bgMass,90)
                except:
                    perc90_mass = '???'
                self.perc90dic[cas] = perc90_mass
            with open('perc90dic.pkl','wb') as f:
                pickle.dump(self.perc90dic,f)
        else:
            with open('perc90dic.pkl','rb') as f:
                self.perc90dic = pickle.load(f)


    def make_catalog(self,cattype='sort_by_cas',perc90dic=None):
        print(f'\n  ** working on {cattype} **')
        self.save_global_vals(self.num_events)
        tt = self.allrec.groupby('bgCAS',as_index=False)['bgIngredientName'].count()
        tt.columns = ['bgCAS','cnt']

        t = self.tab_man.tables['cas'].get_df()
        if self.caslist != []:
            t = t[t.bgCAS.isin(self.caslist)]
        t = pd.merge(t,tt,on='bgCAS',how='left')
        t.eh_Class_L1 = np.where(t.eh_Class_L1.isna(),
                                 'not yet classified',
                                 t.eh_Class_L1)
        t.eh_Class_L2 = np.where(t.eh_Class_L2.isna(),
                                 'not yet classified',
                                 t.eh_Class_L2)
        gb = t.groupby('bgCAS',as_index=False)['bgIngredientName',
                                               'eh_Class_L1',
                                               'eh_Class_L2',
                                               'is_on_TEDX',
                                               'is_on_TSCA',
                                               'cnt'].first()
        #gb = gb[:10]
        if cattype == 'sort_by_cas':
            fn = 'sort_by_cas.html'
            title = 'open_FF Catalog:  sorted by CAS Number'
        if cattype == 'TEDX_only':
            fn = 'TEDX_only.html'
            title = 'open_FF Catalog: On TEDX list (sorted by CAS)'
            gb = gb[gb.is_on_TEDX]
        if cattype == 'sort_by_eh_class':
            fn = 'by_eh_class.html'
            title = 'open_FF Catalog: sorted by Elsner/Hoelzer class'
            gb.sort_values(['cnt'],ascending=False,inplace=True)
            gb.sort_values(['eh_Class_L1','eh_Class_L2'],inplace=True)
        if cattype == 'sort_by_num_records':
            fn = 'by_num_records.html'
            title = 'open_FF Catalog: sorted by number of records'
            gb.sort_values(['cnt'],inplace=True,ascending=False)
            
        

        # start table
        s = '<table>'
        s += self.add_table_head(['report','data file',
                                  'CAS number','name','number of records',
                                  'upper 10% mass >',
                                  'on TEDX?','on TSCA?','eh Class L1',
                                  'eh Class L2','ChemID link'])
        for (i, row) in gb.iterrows():
            chem = row.bgCAS
            #print(f'\nWorking on ** {chem} **  ({i})')
            ingred = row.bgIngredientName
#            self.save_global_vals(num_events,chem,ingred,today)
            
            #tt = allrec[allrec.bgCAS==chem]
            #tt.to_csv(self.outdir+chem+'/data.csv',index=False)
            #tt.to_csv('website_gen/data.csv',index=False)
            if row.cnt>0: # there are records for this chem
                pass
            else: # it is in CAS table but no filtered records of it
                continue

            
            # add text to index body
            try:
                per90 = round_sig(self.perc90dic[chem],1)
            except:
                per90 = 'XXX'
            linelst = [f'<a href= {chem}/analysis_{chem}.html> analysis </a>',
                       f'<a href= {chem}/data.csv> filtered data </a>',
#                       f'<div class="cas"><a href={chem+"/"+chem+".html"}>{chem} </a></div>',
                       f'<div class="cas">{chem}</div>',
                       f'{ingred}',
#                       f'{numrecs}',
                       f'{int(row.cnt)}',
                       f'{per90}',
                       f'{row.is_on_TEDX}',
                       f'{row.is_on_TSCA}',
                       f'{row.eh_Class_L1}',
                       f'{row.eh_Class_L2}',
                       f'<a target="_blank" href=https://chem.nlm.nih.gov/chemidplus/rn/{chem}> link </a>',
                       ]
            if int(row.cnt) < self.minCount:
                linelst[0] = f'# of records too small'
            s += self.add_table_line(linelst)
        s+= '</table>\n'    
        pg = self.compile_page(title=title,body=s,
                               header=title)
        self.save_page(webtxt=pg,fn=fn)

    def make_all_catalogs(self):
        self.make_10perc_dict(fromScratch=True)
        print(f'Size of perc90dic: {len(self.perc90dic)}')
        self.make_catalog('sort_by_cas')
        self.make_catalog('TEDX_only')
        self.make_catalog('sort_by_eh_class')
        self.make_catalog('sort_by_num_records')
        
    def make_robot_file(self):
        """create robots.txt file"""
        s = "User-agent: * \n"
        s+= "Disallow: / \n"
        self.save_page(webtxt=s,fn='robots.txt')

    def make_front_page(self):
        # start table
        s = "<table>"
        s += self.add_table_head(['Catalog View'])
        s += self.add_table_line([f'<a href=sort_by_cas.html> Sort by CAS number </a>'])
        s += self.add_table_line([f'<a href=by_num_records.html> Sort by Number of Records </a>'])
        s += self.add_table_line([f'<a href=by_eh_class.html> Sort by Elsner/Hoelzer classes </a>'])
        s += self.add_table_line([f'<a href=TEDX_only.html> Show only chemicals on TEDX list </a>'])
        s+= '</table><br><br>\n'    
        s+='For more information about these data, see this website: <a href=https://frackingchemicaldisclosure.wordpress.com/> Unearthing Data About Fracking Chemicals </a> '
        pg = self.compile_page(title='open_FF Catalog',body=s,
                               header='open_FF Catalog home page')
        self.save_page(webtxt=pg,fn='index.html')
        self.make_robot_file()

    def make_jupyter_output(self,subfn=''):
        s= 'jupyter nbconvert --template=nbextensions --ExecutePreprocessor.allow_errors=True --ExecutePreprocessor.timeout=-1 --execute website_gen/chemical_report.ipynb --to=html '
        print(subprocess.run(s))
