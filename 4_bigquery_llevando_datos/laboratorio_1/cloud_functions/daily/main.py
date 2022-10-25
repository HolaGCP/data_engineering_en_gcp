import functions_framework
from functions import main
# Register an HTTP function with the Functions Framework
@functions_framework.http
def my_http_function(request):
  # Your code here

  # Return an HTTP response
  main()
  return 'OK'