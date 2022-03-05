import cv2
import os
from detection import canny_detector

# Tunable parameters must have defaults!
# input_path_dict will have input file paths with keys corresponding to the input names set in the cli.
def run_module(input_path_dict, output_folder_path, min_hysteresis=100, max_hysteresis=200): 

    ##### Preprocessing #####

    # Get input file paths from dictionary
    input_img_path = input_path_dict['Input Image']

    # Load data
    img = cv2.imread(input_img_path, 0)
    
    ##### Run algorithm #####

    edges_detected = canny_detector(img, min_hysteresis, max_hysteresis)
    
   
    ##### Save output #####
    
    # Get filename
    input_img_name = os.path.split(input_img_path)[-1][:-4]
    
    # Generate desired output file names and paths
    output_img_path = "%s/%s_out.jpg" % (output_folder_path, input_img_name)

    # Save output files
    cv2.imwrite(output_img_path, edges_detected)

    # Create dictionary of output paths
    output_paths_dict = {}
    output_paths_dict['Output Image'] = output_img_path
    
    ##### Return output file path #####  -> Important step
    return output_paths_dict
    
if __name__ == '__main__':
    # Place some code to test implementation
    pass
     
