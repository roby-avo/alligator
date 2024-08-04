from fastapi import Request

async def trace_ip(request: Request):
    ip_address = request.client.host
    # Add logic to trace IP addresses and manage rate limiting if necessary
    print(f"IP Address: {ip_address}")
    return ip_address
