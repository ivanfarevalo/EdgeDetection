import sys
# import io
from lxml import etree
import xml.etree.ElementTree as ET
import optparse
import logging
import os
# import numpy as np

# It is importing from source

logging.basicConfig(filename='PythonScript.log', filemode='a', level=logging.DEBUG)
log = logging.getLogger('bq.modules')

# from bqapi.comm import BQCommError
from bqapi.comm import BQSession
from bqapi.util import fetch_blob

# standardized naming convention for running modules.
from src.BQ_run_module import run_module


class ScriptError(Exception):
    def __init__(self, message):
        self.message = "Script error: %s" % message

    def __str__(self):
        return self.message


class PythonScriptWrapper(object):
    def __init__(self):
        for file in os.listdir(): # Might change it to read parameters from .JSON or from modulePath variable
            if file.endswith(".xml"):
                # Get xml file name as module name
                if hasattr(self, 'module_name'):
                    raise ScriptError('More than 1 .xml file present in directory, make appropiate changes and rebuild image')
                else:
                    self.module_name = file[:-4]

        tree = ET.parse(self.module_name+'.xml')  # Load module xml as tree
        self.root = tree.getroot()  # Get root node of tree


    # For very simple, image in image out case.  Will extend to more input/output cases.
    # def get_xml_data(self, field, out_xml_value='Default', bq=None):
    #     xml_data = []
    #
    #     for node in self.root:  # Iterate tree to parse necessary information
    #         # print(child.tag, child.attrib)
    #         if field == 'inputs' and node.attrib['name'] == 'inputs':
    #
    #             for input in node:
    #                 if input.attrib['name'] == 'resource_url':
    #                     resource_ulr = bq.load(self.options.resource_url)
    #                     resource_name = resource_ulr.__dict__['name']
    #                     resource_dict = {'resource_url': resource_ulr, 'resource_name':resource_name}
    #                     xml_data.append(resource_dict)
    #
    #         elif field == 'outputs' and node.attrib['name'] == 'outputs':
    #
    #             for output in node:
    #                 if output.attrib['name'] == 'OutImage':
    #                     output.set('value', out_xml_value)
    #                     output_xml = ET.tostring(output).decode('utf-8')
    #                     xml_data.append(output_xml)
    #
    #     log.info(f" xml data for {field} from wrapper is {xml_data}")
    #     return xml_data




    # log.debug('kw is: %s', str(kw))
    # predictor_uniq = predictor_url.split('/')[-1]
    # reducer_uniq = reducer_url.split('/')[-1]
    # table_uniq = table_url.split('/')[-1]
    #
    # predictor_url = bq.service_url('blob_service', path=predictor_uniq)
    # predictor_path = os.path.join(kw.get('stagingPath', ''), 'predictor.sav')
    # predictor_path = bq.fetchblob(predictor_url, path=predictor_path)
    #
    # reducer_url = bq.service_url('blob_service', path=reducer_uniq)
    # reducer_path = os.path.join(kw.get('stagingPath', ''), 'reducer.sav')
    # reducer_path = bq.fetchblob(reducer_url, path=reducer_path)

    
    
    def upload_results(self, bq):
        """
        Reads output specs from xml and uploads results to Bisque using correct service
        """

        output_resources = []
        non_image_value = {}
        
        # Get outputs tag and its nonimage child tag
        outputs_tag = self.root.find("./*[@name='outputs']")
        print(outputs_tag)
        nonimage_tag = outputs_tag.find("./*[@name='NonImage']")
        print(nonimage_tag.tag, nonimage_tag.attrib)
        
        # Upload each resource with the corresponding service
        for resource in (nonimage_tag.findall(".//*[@type]") + outputs_tag.findall("./*[@type='image']")): 
            print(resource.tag, resource.attrib)
            print("NonImage type output with name %s" % resource.attrib['name'])
            resource_name = resource.attrib['name']
            resource_type = resource.attrib['type']
            resource_path = self.output_data_path_dict[resource_name]
            log.info(f"***** Uploading output {resource_type} '{resource_name}' from {resource_path} ...")

            # Upload output resource to Bisque and get resource etree.Element
            output_etree_Element = self.upload_service(bq, resource_path, data_type=resource_type)
            log.info(f"***** Uploaded output {resource_type} '{resource_name}' to {output_etree_Element.get('value')}")

            # Set the value attribute of the each resource's tag to its corresponding resource uri
            resource.set('value', output_etree_Element.get('value'))
            
            # Append image outputs to output resources list
            if resource in outputs_tag.findall("./*[@type='image']"):
                output_resource_xml = ET.tostring(resource).decode('utf-8')
                output_resources.append(output_resource_xml)
            else:
                non_image_value[resource_name] = output_etree_Element.get('value')
        # Append all nonimage outputs to output resource list
#        output_resource_xml = ET.tostring(nonimage_tag).decode('utf-8')
#        output_resources.append(output_resource_xml)

        template_tag = nonimage_tag.find("./template")
        nonimage_tag.remove(template_tag)
        for resource in non_image_value:
            ET.SubElement(nonimage_tag, 'tag', attrib={'name' : f"{resource}", 'type': 'resource', 'value': f"{non_image_value[resource]}"})

        output_resource_xml = ET.tostring(nonimage_tag).decode('utf-8')
        output_resources.append(output_resource_xml)

#        log.info(f"***** non_image_value = {non_image_value}")
#        outxml = f"""<tag name="NonImage">
#                          <tag name="CSV Out" type="resource" value="{non_image_value['CSV Out']}"/>
#                          <tag name="Npy Out" type="resource" value="{non_image_value['Npy Out']}"/>
#                  </tag>""".replace('\n',' ')
#
#        output_resources.insert(0, outxml)



#        outxml = f"""<tag name="NonImage">
#                          <tag name="label" value="Outputs"/>
#                          <tag name="CSV Out" type="table" value="{non_image_value['CSV Out']}"/>
#                          <tag name="Npy Out" type="file" value="{non_image_value['Npy Out']}"/>
#                  </tag>"""
        
        log.debug(f"***** Output Resources xml : output_resources = {output_resources}")
        # SAMPLE LOG
        # ['<tag name="OutImage" type="image" value="http://128.111.185.163:8080/data_service/00-ExhzBeQiaX5F858qNjqXzM">\n               <template>\n                    <tag name="label" value="Edge Image" />\n               </template>\n          </tag>\n     ']
        return output_resources
    
    
    
    def get_xml_outputs(self, out_xml_value):
        xml_data = []

        for node in self.root:  # Iterate tree to parse necessary information
            # print(node.tag, node.attrib)
            if node.attrib['name'] == 'outputs':
                for output in node:
                    try:
                        if (output.attrib['type'] == 'image'):
                            output.set('value', out_xml_value)
                            output_xml = ET.tostring(output).decode('utf-8')
                            xml_data.append(output_xml)
                        elif (output.attrib['type'] == 'table'):
                            
                            output.set('value', out_xml_value)
                            output_xml = ET.tostring(output).decode('utf-8')
                            xml_data.append(output_xml)
                        elif (output.attrib['type'] == 'resource'):
                            
                            output.set('value', out_xml_value)
                            output_xml = ET.tostring(output).decode('utf-8')
                            xml_data.append(output_xml)
                    except KeyError: # If no type mentioned
                        
                        output.set('value', out_xml_value)
                        output_xml = ET.tostring(output).decode('utf-8')
                        xml_data.append(output_xml)

        log.info(f"***** Output XML data: {xml_data}")
        return xml_data


    def fetch_input_resources(self, bq, inputs_dir_path): #TODO Not hardcoded resource_url
        """
        Reads input resources from xml, fetches them from Bisque, and copies them to module container for inference

        """

        log.info('***** Options: %s' % (self.options))
        
        input_bq_objs = []
        input_path_dict = {} # Dictionary that contains the paths of the input resources
        
        inputs_tag = self.root.find("./*[@name='inputs']")
#        print(inputs_tag)
        for input_resource in inputs_tag.findall("./*[@type='resource']"):
            # for child in node.iter():
            print(input_resource.tag, input_resource.attrib)

            input_name = input_resource.attrib['name']
            log.info(f"***** Processing resource named: {input_name}")
            resource_obj = bq.load(getattr(self.options, input_name))
            """
            bq.load returns bqapi.bqclass.BQImage object. Ex:
            resource_obj: (image:name=whale.jpeg,value=file://admin/2022-02-25/whale.jpeg,type=None,uri=http://128.111.185.163:8080/data_service/00-pkGCYS4SPCtQVcdZUUj4sX,ts=2022-02-25T17:05:13.289578,resource_uniq=00-pkGCYS4SPCtQVcdZUUj4sX)

            resource_obj: (resource:name=yolov5s.pt,type=None,uri=http://128.111.185.163:8080/data_service/00-D9e6xVPhU93JtZjZZtwkLm,ts=2022-02-26T01:08:26.198330,resource_uniq=00-D9e6xVPhU93JtZjZZtwkLm) (PythonScriptWrapper.py:137)

            resource_obj: (resource:name=test.npy,type=None,uri=http://128.111.185.163:8080/data_service/00-EC53Rcbj8do86aXpea2cgW,ts=2022-02-26T01:17:12.312780,resource_uniq=00-EC53Rcbj8do86aXpea2cgW) (PythonScriptWrapper.py:137)
            """

            input_bq_objs.append(resource_obj)
            log.info(f"***** resource_obj: {resource_obj}")
            log.info(f"***** resource_obj.uri: {resource_obj.uri}")
            log.info(f"***** type(resource_obj): {type(resource_obj)}")

            # Append uri to dictionary of input paths
            input_path_dict[input_name] = os.path.join(inputs_dir_path, resource_obj.name)

            # Saves resource to module container at specified dest path
            fetch_blob_output = fetch_blob(bq, resource_obj.uri, dest=input_path_dict[input_name])
            log.info(f"***** fetch_blob_output: {fetch_blob_output}") 
        
        log.info(f"***** Input path dictionary : {input_path_dict}")

        return input_path_dict

            

#        for node in self.root:  # Iterate tree to parse necessary information
#            # print(node.tag, node.attrib)
#            if node.attrib['name'] == 'inputs':
#
#                for child in node.iter():
#                    # <tag name="resource_url" type="resource">
#                    # input_name = ''
#
#                    try:
#                        if (child.attrib['name'] and child.attrib['type'] == 'resource'):
#                            input_name = child.attrib['name']
#                    except KeyError:
#                        pass
#
#                    try:
#                        if (child.attrib['name'] == 'accepted_type' and child.attrib['value'] == 'image'):
#                            print("INPUT OF TYPE IMAGE!")
#
#                            # resource_url = bq.load(self.options.resource_url)
#
##                            log.info(f"***** self.options.resource_url: {self.options.resource_url}")
#                            
#                            resource_obj = bq.load(getattr(self.options, input_name))
#                            # bq.load returns bqapi.bqclass.BQImage object. Ex:
#                            # resource_obj: (image:name=whale.jpeg,value=file://admin/2022-02-25/whale.jpeg,type=None,uri=http://128.111.185.163:8080/data_service/00-pkGCYS4SPCtQVcdZUUj4sX,ts=2022-02-25T17:05:13.289578,resource_uniq=00-pkGCYS4SPCtQVcdZUUj4sX)
#
#                            # ***** resource_obj: (resource:name=yolov5s.pt,type=None,uri=http://128.111.185.163:8080/data_service/00-D9e6xVPhU93JtZjZZtwkLm,ts=2022-02-26T01:08:26.198330,resource_uniq=00-D9e6xVPhU93JtZjZZtwkLm) (PythonScriptWrapper.py:137)
#
#                            # resource_obj: (resource:name=test.npy,type=None,uri=http://128.111.185.163:8080/data_service/00-EC53Rcbj8do86aXpea2cgW,ts=2022-02-26T01:17:12.312780,resource_uniq=00-EC53Rcbj8do86aXpea2cgW) (PythonScriptWrapper.py:137)
#                            
#                            resource_name = resource_obj.name
##                            resource_name = resource_obj.__dict__['name']
#
#                            xml_data.append(resource_obj)
#                            resource_dict = {'resource_obj': resource_obj, 'resource_name': resource_name}
##                            xml_data.append(resource_dict)
#                            log.info(f"***** resource_dict['resource_obj'].uri: {resource_dict['resource_obj'].uri}")
#                            log.info(f"***** resource_obj: {resource_obj}")
#                            log.info(f"***** resource_dict['resource_obj']: {resource_dict['resource_obj']}")
#                            log.info(f"***** type(resource_dict['resource_obj']): {type(resource_dict['resource_obj'])}")
##                            log.info(f"***** resource_dict['resource_obj']: {resource_dict['resource_obj']}")
#                        elif (child.attrib['name'] == 'accepted_type' and child.attrib['value'] == 'file'):
#                            
#                            log.info(f"***** getattr(self.options, input_name): {getattr(self.options, input_name)}")
#                            
#                            resource_obj = bq.load(getattr(self.options, input_name))
#                            xml_data.append(resource_obj)
#                            resource_name = resource_obj.name
#                            resource_dict = {'resource_obj': resource_obj, 'resource_name': resource_name}
##                            xml_data.append(resource_dict)
#                            log.info(f"***** resource_dict['resource_obj'].uri: {resource_dict['resource_obj'].uri}")
#                            log.info(f"***** resource_obj: {resource_obj}")
#                            log.info(f"***** resource_dict['resource_obj']: {resource_dict['resource_obj']}")
#                            log.info(f"***** type(resource_dict['resource_obj']): {type(resource_dict['resource_obj'])}")
#
#                    except KeyError:
#                        pass
#
#        log.info(f"***** Input XML data: {xml_data}")
#        # SAMPLE LOG
#        # INFO:bq.modules:***** Input XML data: [{'resource_obj': (image:http://128.111.185.163:8080/data_service/00-pkGCYS4SPCtQVcdZUUj4sX), 'resource_name': 'whale.jpeg'}]

    def pre_process(self, bq):
        """
        Ingests and logs xml file inputs and outputs

        :param bq:
        :return:
        """

        log.info('Options: %s' % (self.options))

        self.input_resource_objs = self.fetch_input_resources(bq=bq)

        # Saves and log input
        for input in self.input_resource_objs:

            log.info("Process resource as %s" % input.name)
            log.info("Resource meta: %s" % input.uri)
            cwd = os.getcwd()
            log.info("Current work directory: %s" % (cwd))

            # SAMPLE LOG
            # INFO:bq.modules:Process resource as whale.jpeg
            # INFO:bq.modules:Resource meta: (image:name=whale.jpeg,value=file://admin/2022-02-25/whale.jpeg,type=None,uri=http://128.111.185.163:8080/data_service/00-pkGCYS4SPCtQVcdZUUj4sX,ts=2022-02-25T17:05:13.289578,resource_uniq=00-pkGCYS4SPCtQVcdZUUj4sX)
            # INFO:bq.modules:Current work directory: /module

            # Saves resource to module container
            result = fetch_blob(bq,input.uri, dest=os.path.join(cwd, input.name))
            # result = fetch_blob(bq, self.options.resource_obj, dest=os.path.join(cwd, input['resource_name']))
            log.info(f"input.uri: {input.uri}")
            log.info(f"Output of fetch blob in pre_process : {result}")

            # SAMPLE LOG
            # INFO:bq.modules:Output of fetch blob in pre_process : {'http://128.111.185.163:8080/data_service/00-pkGCYS4SPCtQVcdZUUj4sX': './whale.jpeg'}


    def run(self):
        """
        Run Python script

        """
        bq = self.bqSession
        log.info('***** self.options: %s' % (self.options))
        
        # Use current directory to store input and output data for now, if changed, might have to look at teardown funct too
        inputs_dir_path = os.getcwd() 
        outputs_dir_path = os.getcwd() 

        # Fetch input resources
        try:
            bq.update_mex('Fetching inputs specified in xml')
            input_path_dict = self.fetch_input_resources(bq, inputs_dir_path)
        except (Exception, ScriptError) as e:
            log.exception("***** Exception while fetching inputs specified in xml")
            bq.fail_mex(msg="Exception while fetching inputs specified in xml: %s" % str(e))
            return

        
        # Run module from BQ_run_module and get get a dictionary that contains the paths to the module results
        try:
            bq.update_mex('Running module')
            self.output_data_path_dict = run_module(input_path_dict, outputs_dir_path) 
        except (Exception, ScriptError) as e:
            log.exception("***** Exception while running module from BQ_run_module")
            bq.fail_mex(msg="Exception while running module from BQ_run_module: %s" % str(e))
            return

        # Upload results to Bisque
        try:
            bq.update_mex('Uploading results to Bisque')
            self.output_resources = self.upload_results(bq)
        except (Exception, ScriptError) as e:
            log.exception("***** Exception while uploading results to Bisque")
            bq.fail_mex(msg="Exception while uploading results to Bisque: %s" % str(e))
            return
        
        
#        for output_name in self.output_data_path_dict:
#            log.info(f"***** Output data path for '{output_name}': {self.output_data_path_dict[output_name]}")
#            
#        # output_folder_path = os.path.join(os.path.dirname(os.getcwd()), 'outputs')
#        output_folder_path = os.getcwd()
#
#        out_data_path = run_module(input_file_path, output_folder_path)  # Path to output files HARDCODED FOR NOW
#        log.info("Output image path: %s" % out_data_path)
#
#        # SAMPLE LOG
#        # INFO:bq.modules:Output image path: /module/whale._out.jpg
#
#        self.bqSession.update_mex('Returning results')
#
#        bq.update_mex('Uploading Mask result')
#
#        self.out_image = self.upload_service(bq, out_data_path, data_type='file')
##        self.out_image = self.upload_service(bq, out_data_path, data_type='image')
#        #         log.info('Total number of slices:{}.\nNumber of slices predicted as Covid:{}.\nNumber of slices predicted as PNA: {}\nNumber of slices predicted as Normal:{}'.format(z, covid, pna, normal))
#
#        #         self.output_resources.append(out_xml)
#
##        self.output_resources = self.get_xml_outputs(out_xml_value=(self.out_image.get('value')))
#
#
##        self.output_resources = ["""<tag name="npy_out" type="resource" value="%s">
##                                            <template>
##                                                <tag name="label" value="npy out"/>
##                                            </template>
##                                     </tag>""" % self.out_image.get('value')]
#
#        self.output_resources = ["""<tag name="Outputs">
#                                                <tag name="Npy Out" type="resource" value="%s"/>
#                                                <tag name="npy_out2" type="resource" value="%s"/>
#                                     </tag>""" % (self.out_image.get('value'), self.out_image.get('value'))]
#
##        self.output_resources.append("""<tag name="Metadata2">
##                                                <tag name="npy_out3" type="resource" value="%s"/>
##                                                <tag name="npy_out4" type="resource" value="%s"/>
##                                     </tag>""" % (self.out_image.get('value'), self.out_image.get('value')))
#
##        self.output_resources = ["""<tag name="npy_out" type="resource" value="%s"/>""" % self.out_image.get('value')]
#
##        self.output_resources = [f"""<tag name="Metadata"\n               <tag name="npy_out" type="resource" value="{self.out_image.get('value')}"/>\n          </tag>\n"""]
##        self.output_resources = [f"""<tag name="Metadata" <tag name="npy_out" type="resource" value="{self.out_image.get('value')}"/> </tag>"""]
#
##        self.output_resources = ["""<tag name="Metadata">
##                                            <tag name="Volumes Table" type="resource" value="%s"/>
##                                    </tag>""" % self.out_image.get('value')]
#
#        # self.output_resources = self.get_xml_data('outputs', out_xml_value=(str(self.out_image.get('value'))))
#
#        # out_imgxml = """<tag name="EdgeImage" type="image" value="%s">
#        #                 <template>
#        #                   <tag name="label" value="Edge Image" />
#        #                 </template>
#        #               </tag>""" % (str(self.out_image.get('value')))
#
#        #        out_xml = """<tag name="Metadata">
#        #                    <tag name="Filename" type="string" value="%s"/>
#        #                    <tag name="Depth" type="string" value="%s"/>
#        #                     <tag name="Covid" type="string" value="%s"/>
#        #                     <tag name="Pneumonia" type="string" value="%s"/>
#        #                     <tag name="normal" type="string" value="%s"/>
#        #                     </tag>""" % (self.image_name, str(z), str(covid), str(pna), str(normal))
#
#        #        outputs = [out_imgxml, out_xml]
#        #         outputs = [out_imgxml]
#        log.debug(f"***** self.output_resources = {self.output_resources}")
#        # SAMPLE LOG
#        # ['<tag name="OutImage" type="image" value="http://128.111.185.163:8080/data_service/00-ExhzBeQiaX5F858qNjqXzM">\n               <template>\n                    <tag name="label" value="Edge Image" />\n               </template>\n          </tag>\n     ']
#
#
#        # save output back to BisQue
#        # for output in outputs:
#        #     self.output_resources.append(output)

    def setup(self):
        """
        Pre-run initialization
        """
        self.bqSession.update_mex('Initializing...')
        self.mex_parameter_parser(self.bqSession.mex.xmltree)
        self.output_resources = []

    def tear_down(self):  # NEED TO GENERALIZE
        """
        Post the results to the mex xml
        """
        self.bqSession.update_mex('Returning results')
        outputTag = etree.Element('tag', name='outputs')
        for r_xml in self.output_resources:
            if isinstance(r_xml, str):
                r_xml = etree.fromstring(r_xml)
            res_type = r_xml.get('type', None) or r_xml.get(
                'resource_type', None) or r_xml.tag
            # append reference to output
            if res_type in ['table', 'image']:
                outputTag.append(r_xml)
                # etree.SubElement(outputTag, 'tag', name='output_table' if res_type=='table' else 'output_image', type=res_type, value=r_xml.get('uri',''))
            else:
                outputTag.append(r_xml)
                # etree.SubElement(outputTag, r_xml.tag, name=r_xml.get('name', '_'), type=r_xml.get('type', 'string'), value=r_xml.get('value', ''))
        log.debug('Output Mex results: %s' %
                  (etree.tostring(outputTag, pretty_print=True)))
        self.bqSession.finish_mex(tags=[outputTag])

    def mex_parameter_parser(self, mex_xml):
        """
            Parses input of the xml and add it to options attribute (unless already set)

            @param: mex_xml
        """
        # inputs are all non-"script_params" under "inputs" and all params under "script_params"
        mex_inputs = mex_xml.xpath(
            'tag[@name="inputs"]/tag[@name!="script_params"] | tag[@name="inputs"]/tag[@name="script_params"]/tag')
        if mex_inputs:
            for tag in mex_inputs:
                if tag.tag == 'tag' and tag.get('type', '') != 'system-input':  # skip system input values
                    if not getattr(self.options, tag.get('name', ''), None):
                        log.debug('Set options with %s as %s' % (tag.get('name', ''), tag.get('value', '')))
                        setattr(self.options, tag.get('name', ''), tag.get('value', ''))
        else:
            log.debug('No Inputs Found on MEX!')

    def uploadimgservice(self, bq, filename):
        """
        Upload mask to image_service upon post process
        """
        mex_id = bq.mex.uri.split('/')[-1]

        log.info('Up Mex: %s' % (mex_id))
        log.info('Up File: %s' % (filename))
        resource = etree.Element(
            'image', name='ModuleExecutions/EdgeDetection/' + filename)
        t = etree.SubElement(resource, 'tag', name="datetime", value='time')
        log.info('Creating upload xml data: %s ' %
                 str(etree.tostring(resource, pretty_print=True)))
        # os.path.join("ModuleExecutions","CellSegment3D", filename)
        filepath = filename
        # use import service to /import/transfer activating import service
        r = etree.XML(bq.postblob(filepath, xml=resource)).find('./')
        if r is None or r.get('uri') is None:
            bq.fail_mex(msg="Exception during upload results")
        else:
            log.info('Uploaded ID: %s, URL: %s' %
                     (r.get('resource_uniq'), r.get('uri')))
            bq.update_mex('Uploaded ID: %s, URL: %s' %
                          (r.get('resource_uniq'), r.get('uri')))
            self.furl = r.get('uri')
            self.fname = r.get('name')
            resource.set('value', self.furl)

        return resource

    def upload_service(self, bq, filename, data_type='image'):
        """
        Upload resource to specific service upon post process
        """
        mex_id = bq.mex.uri.split('/')[-1]

        log.info('Up Mex: %s' % (mex_id))
        log.info('Up File: %s' % (filename))
        resource = etree.Element(
            data_type, name='ModuleExecutions/' + self.module_name + '/' + filename)
        t = etree.SubElement(resource, 'tag', name="datetime", value='time')
        log.info('Creating upload xml data: %s ' %
                 str(etree.tostring(resource, pretty_print=True)))
        # os.path.join("ModuleExecutions","CellSegment3D", filename)
        filepath = filename
        # use import service to /import/transfer activating import service
        r = etree.XML(bq.postblob(filepath, xml=resource)).find('./')
        if r is None or r.get('uri') is None:
            bq.fail_mex(msg="Exception during upload results")
        else:
            log.info('Uploaded ID: %s, URL: %s' %
                     (r.get('resource_uniq'), r.get('uri')))
            bq.update_mex('Uploaded ID: %s, URL: %s' %
                          (r.get('resource_uniq'), r.get('uri')))
            self.furl = r.get('uri')
            self.fname = r.get('name')
            resource.set('value', self.furl)

        return resource

    def validate_input(self):
        """
            Check to see if a mex with token or user with password was provided.

            @return True is returned if validation credention was provided else
            False is returned
        """
        if (self.options.mexURL and self.options.token):  # run module through engine service
            return True
        if (self.options.user and self.options.pwd and self.options.root):  # run module locally (note: to test module)
            return True
        log.debug('Insufficient options or arguments to start this module')
        return False

    def main(self):
        parser = optparse.OptionParser()
        parser.add_option('--mex_url', dest="mexURL")
        parser.add_option('--module_dir', dest="modulePath")
        parser.add_option('--staging_path', dest="stagingPath")
        parser.add_option('--bisque_token', dest="token")
        parser.add_option('--user', dest="user")
        parser.add_option('--pwd', dest="pwd")
        parser.add_option('--root', dest="root")
        # parser.add_option('--resource_url', dest="resource_url")

        (options, args) = parser.parse_args()

        fh = logging.FileHandler('scriptrun.log', mode='a')
        fh.setLevel(logging.DEBUG)
        formatter = logging.Formatter('[%(asctime)s] %(levelname)8s --- %(message)s ' +
                                      '(%(filename)s:%(lineno)s)', datefmt='%Y-%m-%d %H:%M:%S')
        fh.setFormatter(formatter)
        log.addHandler(fh)

        try:  # pull out the mex

            if not options.mexURL:
                options.mexURL = sys.argv[-2]
            if not options.token:
                options.token = sys.argv[-1]
        except IndexError:  # no argv were set
            pass

        if not options.stagingPath:
            options.stagingPath = ''

        log.debug('\n\nPARAMS : %s \n\n Options: %s' % (args, options))
        self.options = options

        if self.validate_input():

            # initalizes if user and password are provided
            if (self.options.user and self.options.pwd and self.options.root):

                try:
                    self.bqSession = BQSession().init_local(self.options.user, self.options.pwd,
                                                            bisque_root=self.options.root)
                    self.options.mexURL = self.bqSession.mex.uri

                except:
                    return

            # initalizes if mex and mex token is provided
            elif (self.options.mexURL and self.options.token):

                try:
                    self.bqSession = BQSession().init_mex(self.options.mexURL, self.options.token)
                except:
                    return



            else:
                raise ScriptError('Insufficient options or arguments to start this module')

            try:
                self.setup()
            except Exception as e:
                log.exception("Exception during setup")
                self.bqSession.fail_mex(msg="Exception during setup: %s" % str(e))
                return
            ####
            try:
                self.run()
            except (Exception, ScriptError) as e:
                log.exception("Exception during run")
                self.bqSession.fail_mex(msg="Exception during run: %s" % str(e))
                return
            ##
            try:
                self.tear_down()
            except (Exception, ScriptError) as e:
                log.exception("Exception during tear_down")
                self.bqSession.fail_mex(msg="Exception during tear_down: %s" % str(e))
                return

            self.bqSession.close()
        log.debug('Session Close')


if __name__ == "__main__":
    PythonScriptWrapper().main()
