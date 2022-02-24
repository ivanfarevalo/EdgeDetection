import cv2
import os
from detection import canny_detector

# Tunable parameters must have defaults!
def run_module(input_file_path, output_folder_path, min_hysteresis=100, max_hysteresis=200): 

    ##### Preprocessing #####
    img = cv2.imread(input_file_path, 0)
    
    ##### Run algorithm #####
    edges_detected = canny_detector(img, min_hysteresis, max_hysteresis)
    
    ##### Save output #####
    
    # Get filename
    file_name = os.path.split(input_file_path)[-1][:-4]
    
    # Generate desired output file name and path
    output_file_path = "%s/%s_out.jpg" % (output_folder_path, file_name)
    cv2.imwrite(output_file_path, edges_detected)
    
    ##### Return output file path #####  -> Important step
    return output_file_path
    
if __name__ == '__main__':
    # Place some code to test implementation
     
    root_folder_path = os.path.dirname(os.path.abspath(__file__)) # Absolute path to parent directory 
    input_file_path = os.path.join(root_folder_path, 'bob.jpg')
    output_folder_path = root_folder_path

    output_file_path = run_module(input_file_path, output_folder_path, 100, 200)
    print("Output file saved to: %s" % output_file_path)
    
