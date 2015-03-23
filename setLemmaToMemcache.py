#-*- coding: utf-8 -*-
__author__ = 'wangqiang'

import sys
import os
import time
from multiprocessing import Process
import memcache
from xml.etree import ElementTree
from xml.etree.ElementTree import Element, SubElement
from xml.dom import minidom


def rewriteXml(xmlfile):
    if xmlfile is None or len(xmlfile) < 1:
        return None
    try:
        dom = ElementTree.parse(xmlfile)
        root = dom.getroot()

        nroot = Element('lemmaInfo')
        nlemmaId = SubElement(nroot, 'lemmaId')
        nlemmaTitle = SubElement(nroot, 'lemmaTitle')
        npicUrl = SubElement(nroot, 'picUrl')
        npicWidth = SubElement(nroot, 'picWidth')
        npicHeight = SubElement(nroot, 'picHeight')
        nlemmaAbstract = SubElement(nroot, 'lemmaAbstract')
        nparagraphs = SubElement(nroot, 'paragraphs')

        #填充数据
        nlemmaId.text = root.find('lemmaId').text
        nlemmaTitle.text = root.find('lemmaTitle').text
        npicUrl.text = root.find('picUrl').text
        npicHeight.text = root.find('picHeight').text
        npicWidth.text = root.find('picWidth').text
        nlemmaAbstract.text = root.find('lemmaAbstract').text

        #解析多个paragraphs
        paragraphsItems = root.findall('paragraphs/paragraph')
        end = 8
        if end > len(paragraphsItems):
            end = len(paragraphsItems)
        for index in range(0, end):
            paragraph = paragraphsItems[index]
            nparagraph = SubElement(nparagraphs, 'paragraph')
            nparagraph.append(paragraph.find('paragraphId'))
            nparagraph.append(paragraph.find('paragraphLevel'))
            nparagraph.append(paragraph.find('paragraphTitle'))

        nxml_string = ElementTree.tostring(nroot)
        ntree = minidom.parseString(nxml_string)
        return ntree.toxml(encoding='utf-8').replace('\t', '').replace('\n', '')

    except Exception:
        pass
    return None

if __name__ == '+__main__':
    xmlfile = 'lemma/460058484.xml'
    rewritexml = rewriteXml(xmlfile)
    print rewritexml
    # print len((rewritexml.strip()))
    # root = ElementTree.fromstring(rewritexml)
    # print root.findall('paragraphs/paragraph')[0].find('paragraphTitle').text


def setIntoMemcache(mcaddr, filepath, filelist, pname):
    mc = memcache.Client([mcaddr])
    mc.socket_timeout = 5
    fp = open('logs/error-' + pname, 'w')
    for file in filelist:
        xmlfile = filepath + '/' + file
        if os.path.isfile(xmlfile):
            try:
                key = ''
                if file.rfind('.') > 0:
                    key = file[:file.rfind('.')]
                #读xml，写入memcache
                xmlcontent = rewriteXml(xmlfile)
                if xmlcontent is not None and len(xmlcontent.strip()) > 100:
                    #写memcached
                    print '%s succ, key:%s' % (pname, key)
                    mc.set(key, xmlcontent)
                else:
                    print '%s fail, key:%s' % (pname, key)

            except:
                fp.write(xmlfile + '\n')
                print '%s error: %s' % (pname, xmlfile)


    fp.close()

if __name__ == '__main__':

    if len(sys.argv) < 3:
        print 'parameter error'
    else:
        filepath = sys.argv[1]
        mcaddr = sys.argv[2]

        print 'filepath:%s,\tmcaddr:%s' % (filepath, mcaddr)

        #list file
        files = os.listdir(filepath)
        #多进程，每个线程跑5K个文件
        filecount = len(files)
        processcount = filecount / 5000
        if processcount == 0:
            processcount = 1

        filelists = []
        for tindex in range(0, processcount):
            start = 5000 * tindex
            end = start + 5000
            if end > filecount:
                end = filecount

            filelist = []
            for findex in range(start, end):
                filelist.append(files[findex])

            filelists.append(filelist)

        #创建进程
        plist = []
        for tindex in range(0, processcount):
            p = Process(target=setIntoMemcache, args=(mcaddr, filepath, filelists[tindex], 'process-' + str(tindex)))
            plist.append(p)

        print 'task start running soon, process count:%d' % processcount
        time.sleep(3)
        #启动线程
        for p in plist:
            p.start()
            # join() 会阻塞主线程，相当于多进程串行
            #p.join()