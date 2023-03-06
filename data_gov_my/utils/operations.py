'''
Performs post-operation on data formatting
'''

def perform_operation(data, operation) : 
    if operation == '_REVERSE_' :
        return list(reversed(data))
            
    return data