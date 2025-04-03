"""
Writes excel workbook (archive)
"""

from datetime import datetime
crdt = datetime.utcnow().isoformat()[:19] + 'Z'

fixedArchiveContent = [
    {
        'path': ['[Content_Types].xml'],
        'content': """<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml" /><Default Extension="xml" ContentType="application/xml" /><Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml" /><Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml" /><Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml" /><Override PartName="/xl/theme/theme1.xml" ContentType="application/vnd.openxmlformats-officedocument.theme+xml" /><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml" /><Override PartName="/xl/sharedStrings.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml" /><Override PartName="/docMetadata/LabelInfo.xml" ContentType="application/vnd.ms-office.classificationlabels+xml" /></Types>"""

        # original:
        # 'content': """<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml" /><Default Extension="xml" ContentType="application/xml" /><Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml" /><Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml" /><Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml" /><Override PartName="/xl/theme/theme1.xml" ContentType="application/vnd.openxmlformats-officedocument.theme+xml" /><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml" /><Override PartName="/xl/sharedStrings.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml" /></Types>"""
    },
    {
        'path': ['_rels', '.rels'],
        'content': """<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml" /><Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="docProps/core.xml" /><Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" Target="docProps/app.xml" /><Relationship Id="rId4" Type="http://schemas.microsoft.com/office/2020/02/relationships/classificationlabels" Target="docMetadata/LabelInfo.xml" /></Relationships>"""
        # original:
        # 'content': """<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml" /><Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="docProps/core.xml" /><Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" Target="docProps/app.xml" /></Relationships>"""
    },
    {
        'path': ['docMetadata', 'LabelInfo.xml'],
        'content': """<?xml version="1.0" encoding="utf-8" standalone="yes"?><clbl:labelList xmlns:clbl="http://schemas.microsoft.com/office/2020/mipLabelMetadata"><clbl:label id="{cccd100a-077b-4351-b7ea-99b99562cb12}" enabled="1" method="Privileged" siteId="{f06fa858-824b-4a85-aacb-f372cfdc282e}" contentBits="0" removed="0" /></clbl:labelList>"""
    },
    {
        'path': ['docProps', 'app.xml'],
        'content': """<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties"><Application>Microsoft Excel Compatible / Openpyxl 3.1.5</Application><AppVersion>3.1</AppVersion></Properties>"""
    },
    {
        'path': ['docProps', 'core.xml'],
        'content': """<?xml version="1.0" encoding="UTF-8" standalone="yes"?><cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:dcmitype="http://purl.org/dc/dcmitype/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><dc:creator></dc:creator><cp:lastModifiedBy></cp:lastModifiedBy><dcterms:created xsi:type="dcterms:W3CDTF">{0}</dcterms:created><dcterms:modified xsi:type="dcterms:W3CDTF">{1}</dcterms:modified></cp:coreProperties>""".format(crdt, crdt)
    },
    {
        'path': ['xl', 'styles.xml'],
        'content': """<?xml version="1.0" encoding="UTF-8" standalone="yes"?><styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><fonts count="2"><font><sz val="11"/><color theme="1"/><name val="Calibri"/><family val="2"/><scheme val="minor"/></font><font><b/><sz val="11"/><color theme="1"/><name val="Calibri"/><family val="2"/><scheme val="minor"/></font></fonts><fills count="2"><fill><patternFill patternType="none"/></fill><fill><patternFill patternType="gray125"/></fill></fills><borders count="2"><border><left/><right/><top/><bottom/><diagonal/></border><border><left style="thin"><color auto="1"/></left><right style="thin"><color auto="1"/></right><top style="thin"><color auto="1"/></top><bottom style="thin"><color auto="1"/></bottom><diagonal/></border></borders><cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs><cellXfs count="2"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/><xf numFmtId="0" fontId="1" fillId="0" borderId="1" xfId="0" applyFont="1" applyBorder="1" applyAlignment="1"><alignment horizontal="center" vertical="top"/></xf></cellXfs><cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles><dxfs count="0"/><tableStyles count="0" defaultTableStyle="TableStyleMedium9" defaultPivotStyle="PivotStyleLight16"/></styleSheet>"""
    },
    {
        'path': ['xl', 'theme', 'theme1.xml'],
        'content': """<?xml version="1.0" encoding="UTF-8" standalone="yes"?><theme xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main" name="Office Theme"><themeElements><clrScheme name="Office"><dk1><sysClr val="windowText" lastClr="000000" /></dk1><lt1><sysClr val="window" lastClr="FFFFFF" /></lt1><dk2><srgbClr val="1F497D" /></dk2><lt2><srgbClr val="EEECE1" /></lt2><accent1><srgbClr val="4F81BD" /></accent1><accent2><srgbClr val="C0504D" /></accent2><accent3><srgbClr val="9BBB59" /></accent3><accent4><srgbClr val="8064A2" /></accent4><accent5><srgbClr val="4BACC6" /></accent5><accent6><srgbClr val="F79646" /></accent6><hlink><srgbClr val="0000FF" /></hlink><folHlink><srgbClr val="800080" /></folHlink></clrScheme><fontScheme name="Office"><majorFont><latin typeface="Cambria" /><ea typeface="" /><cs typeface="" /><font script="Arab" typeface="Times New Roman" /><font script="Hebr" typeface="Times New Roman" /><font script="Thai" typeface="Tahoma" /><font script="Ethi" typeface="Nyala" /><font script="Beng" typeface="Vrinda" /><font script="Gujr" typeface="Shruti" /><font script="Khmr" typeface="MoolBoran" /><font script="Knda" typeface="Tunga" /><font script="Guru" typeface="Raavi" /><font script="Cans" typeface="Euphemia" /><font script="Cher" typeface="Plantagenet Cherokee" /><font script="Yiii" typeface="Microsoft Yi Baiti" /><font script="Tibt" typeface="Microsoft Himalaya" /><font script="Thaa" typeface="MV Boli" /><font script="Deva" typeface="Mangal" /><font script="Telu" typeface="Gautami" /><font script="Taml" typeface="Latha" /><font script="Syrc" typeface="Estrangelo Edessa" /><font script="Orya" typeface="Kalinga" /><font script="Mlym" typeface="Kartika" /><font script="Laoo" typeface="DokChampa" /><font script="Sinh" typeface="Iskoola Pota" /><font script="Mong" typeface="Mongolian Baiti" /><font script="Viet" typeface="Times New Roman" /><font script="Uigh" typeface="Microsoft Uighur" /></majorFont><minorFont><latin typeface="Calibri" /><ea typeface="" /><cs typeface="" /><font script="Arab" typeface="Arial" /><font script="Hebr" typeface="Arial" /><font script="Thai" typeface="Tahoma" /><font script="Ethi" typeface="Nyala" /><font script="Beng" typeface="Vrinda" /><font script="Gujr" typeface="Shruti" /><font script="Khmr" typeface="DaunPenh" /><font script="Knda" typeface="Tunga" /><font script="Guru" typeface="Raavi" /><font script="Cans" typeface="Euphemia" /><font script="Cher" typeface="Plantagenet Cherokee" /><font script="Yiii" typeface="Microsoft Yi Baiti" /><font script="Tibt" typeface="Microsoft Himalaya" /><font script="Thaa" typeface="MV Boli" /><font script="Deva" typeface="Mangal" /><font script="Telu" typeface="Gautami" /><font script="Taml" typeface="Latha" /><font script="Syrc" typeface="Estrangelo Edessa" /><font script="Orya" typeface="Kalinga" /><font script="Mlym" typeface="Kartika" /><font script="Laoo" typeface="DokChampa" /><font script="Sinh" typeface="Iskoola Pota" /><font script="Mong" typeface="Mongolian Baiti" /><font script="Viet" typeface="Arial" /><font script="Uigh" typeface="Microsoft Uighur" /></minorFont></fontScheme><fmtScheme name="Office"><fillStyleLst><solidFill><schemeClr val="phClr" /></solidFill><gradFill rotWithShape="1"><gsLst><gs pos="0"><schemeClr val="phClr"><tint val="50000" /><satMod val="300000" /></schemeClr></gs><gs pos="35000"><schemeClr val="phClr"><tint val="37000" /><satMod val="300000" /></schemeClr></gs><gs pos="100000"><schemeClr val="phClr"><tint val="15000" /><satMod val="350000" /></schemeClr></gs></gsLst><lin ang="16200000" scaled="1" /></gradFill><gradFill rotWithShape="1"><gsLst><gs pos="0"><schemeClr val="phClr"><shade val="51000" /><satMod val="130000" /></schemeClr></gs><gs pos="80000"><schemeClr val="phClr"><shade val="93000" /><satMod val="130000" /></schemeClr></gs><gs pos="100000"><schemeClr val="phClr"><shade val="94000" /><satMod val="135000" /></schemeClr></gs></gsLst><lin ang="16200000" scaled="0" /></gradFill></fillStyleLst><lnStyleLst><ln w="9525" cap="flat" cmpd="sng" algn="ctr"><solidFill><schemeClr val="phClr"><shade val="95000" /><satMod val="105000" /></schemeClr></solidFill><prstDash val="solid" /></ln><ln w="25400" cap="flat" cmpd="sng" algn="ctr"><solidFill><schemeClr val="phClr" /></solidFill><prstDash val="solid" /></ln><ln w="38100" cap="flat" cmpd="sng" algn="ctr"><solidFill><schemeClr val="phClr" /></solidFill><prstDash val="solid" /></ln></lnStyleLst><effectStyleLst><effectStyle><effectLst><outerShdw blurRad="40000" dist="20000" dir="5400000" rotWithShape="0"><srgbClr val="000000"><alpha val="38000" /></srgbClr></outerShdw></effectLst></effectStyle><effectStyle><effectLst><outerShdw blurRad="40000" dist="23000" dir="5400000" rotWithShape="0"><srgbClr val="000000"><alpha val="35000" /></srgbClr></outerShdw></effectLst></effectStyle><effectStyle><effectLst><outerShdw blurRad="40000" dist="23000" dir="5400000" rotWithShape="0"><srgbClr val="000000"><alpha val="35000" /></srgbClr></outerShdw></effectLst><scene3d><camera prst="orthographicFront"><rot lat="0" lon="0" rev="0" /></camera><lightRig rig="threePt" dir="t"><rot lat="0" lon="0" rev="1200000" /></lightRig></scene3d><sp3d><bevelT w="63500" h="25400" /></sp3d></effectStyle></effectStyleLst><bgFillStyleLst><solidFill><schemeClr val="phClr" /></solidFill><gradFill rotWithShape="1"><gsLst><gs pos="0"><schemeClr val="phClr"><tint val="40000" /><satMod val="350000" /></schemeClr></gs><gs pos="40000"><schemeClr val="phClr"><tint val="45000" /><shade val="99000" /><satMod val="350000" /></schemeClr></gs><gs pos="100000"><schemeClr val="phClr"><shade val="20000" /><satMod val="255000" /></schemeClr></gs></gsLst><path path="circle"><fillToRect l="50000" t="-80000" r="50000" b="180000" /></path></gradFill><gradFill rotWithShape="1"><gsLst><gs pos="0"><schemeClr val="phClr"><tint val="80000" /><satMod val="300000" /></schemeClr></gs><gs pos="100000"><schemeClr val="phClr"><shade val="30000" /><satMod val="200000" /></schemeClr></gs></gsLst><path path="circle"><fillToRect l="50000" t="50000" r="50000" b="50000" /></path></gradFill></bgFillStyleLst></fmtScheme></themeElements><objectDefaults /><extraClrSchemeLst /></theme>"""
    }
]

def getSheetMetaArchiveContent(sheet_names):
    #
    workbook_xml = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><fileVersion appName="xl" lastEdited="4" lowestEdited="4" rupBuild="4505" /><workbookPr defaultThemeVersion="124226" /><bookViews><workbookView xWindow="240" yWindow="15" windowWidth="16095" windowHeight="9660" /></bookViews><sheets>{0}</sheets><calcPr calcId="124519" fullCalcOnLoad="1" /></workbook>'
    workbook_xml_rels = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">{0}</Relationships>'
    # return this

    workbook_xml_sub = []
    workbook_xml_rels_sub = []

    idx = 1
    for name in sheet_names: 
        workbook_xml_sub.append(
            '<sheet name="{0}" sheetId="{1}" r:id="rId{1}" />'.format(name, idx)
        )
        workbook_xml_rels_sub.append(
            '<Relationship Id="rId{1}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/{0}.xml" />'.format(name, idx)
        )
        idx += 1
    
    workbook_xml_rels_sub += [
        '<Relationship Id="rId{0}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/theme" Target="theme/theme1.xml" />'.format(idx),
	    '<Relationship Id="rId{0}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml" />'.format(idx+1),
	    '<Relationship Id="rId{0}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings" Target="sharedStrings.xml" />'.format(idx+2)
    ]
    return [
        {
            'path': ['xl', 'workbook.xml'],
            'content': workbook_xml.format(''.join(workbook_xml_sub))
        },
        {
            'path': ['xl', '_rels', 'workbook.xml.rels'],
            'content': workbook_xml_rels.format(''.join(workbook_xml_rels_sub))
        }
    ]

# data = sheetMeta(['sheetA', 'sheetB', 'sheetC'])

def getSheetFileContent(table, header=False, columnWidths=[]):
    sheet_xml = '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><dimension ref="A1" /><sheetViews><sheetView workbookViewId="0"></sheetView></sheetViews><sheetFormatPr baseColWidth="8" defaultRowHeight="15" />{0}</worksheet>'
    sheet_xml_sub = ''
    if len(columnWidths):
        sheet_xml_sub += '<cols>'
        idx = 1
        for colwidth in columnWidths:
            sheet_xml_sub += '<col min="{0}" max="{0}" width="{1}"/>'.format(idx, colwidth)
            idx += 1
        sheet_xml_sub += '</cols>'

    if len(table):
        sheet_xml_sub += '<sheetData>'
    else:
        sheet_xml_sub += '<sheetData />'

    row_id = 1
    for row in table:
        row_str = '<row r="{0}">'.format(row_id)
        # code
        for e in row:
            # add s="1" after <c to make bold 
            e_str = '<c t="inlineStr"><is><t>{0}</t></is></c>'.format(e)
            if row_id == 1:
                if header:
                    e_str = '<c s="1" t="inlineStr"><is><t>{0}</t></is></c>'.format(e)
            else:
                if isinstance(e, (int, float)):
                    e_str = '<c t="n"><v>{0}</v></c>'.format(e)
                else:
                    e_str = '<c t="inlineStr"><is><t>{0}</t></is></c>'.format(e)

            row_str += e_str
        row_str += '</row>'

        sheet_xml_sub += row_str
        row_id += 1
    
    if len(table):
        # </sheetData>
        sheet_xml_sub += '</sheetData>'
    return sheet_xml.format(sheet_xml_sub)

def getSheetsArchiveContent(data):
    sheetsArchiveContent = []

    for sheet_name in data:
        table = data[sheet_name]['table']
        if 'header' in data[sheet_name]:
            header = data[sheet_name]['header']
        else:
            header = False
        if 'columnWidths' in data[sheet_name]:
            columnWidths = data[sheet_name]['columnWidths']
        else:
            columnWidths = []

        sheetsArchiveContent.append(
            {
                'path': ['xl', 'worksheets', sheet_name+'.xml'],
                'content': getSheetFileContent(table, header, columnWidths)
            }
        )
    
    return sheetsArchiveContent

from zipfile import ZipFile

def createWorkBook(data, filePath):
    """
    data = {
        'sheet_name': {
            'table' : <2D List>,
            'header': <Bool>,
            'columnWidths': <1D List>
        }
    }
    """
    workBookArchiveContent = []
    workBookArchiveContent += fixedArchiveContent
    workBookArchiveContent += getSheetMetaArchiveContent(data)
    workBookArchiveContent += getSheetsArchiveContent(data)

    # write 1 file for now
    with ZipFile(filePath, 'w') as zipfile:
        for file in workBookArchiveContent:
            path = '/'.join(file['path'])
            zipfile.writestr(path, file['content'])
    return None

SAMPLE_DATA = {
    'sales':{
        'table': [
            ["Product", "Units Sold", "Revenue ($)"],
            ["Laptop", 120, 96000],
            ["Smartphone", 250, 125000],
            ["Tablet", 180, 54000],
            ["Headphones", 300, 45000]
        ],
        'header': True,
        'columnWidths': [20, 10, 10]
    },
    'plants': {
        'table': [
            ["Plant Name", "Type", "Water Needs", "Sunlight"],
            ["Rose", "Flower", "Medium", "Full Sun"],
            ["Cactus", "Succulent", "Low", "Full Sun"],
            ["Fern", "Foliage", "High", "Partial Shade"],
            ["Aloe Vera", "Succulent", "Low", "Bright Indirect"],
            ["Tulip", "Flower", "Medium", "Full Sun"]
        ]
    }
}