# Railway build fix

The previous deployment failed while compiling aiohttp 3.8.5 against Python 3.12.
This version pins aiohttp to 3.10.11 and pins the Railway/Nixpacks runtime to Python 3.11.9.

Deploy by pushing these files to GitHub and redeploying the Railway service.
