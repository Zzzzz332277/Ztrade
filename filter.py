import scipy
input=[0.0,0.0,0.0,0.0,0.0,0.0,1.0,0.0,0.0,0.0,0.0,0.0,0.0]
output=scipy.ndimage.filters.gaussian_filter1d(input,2)
pass