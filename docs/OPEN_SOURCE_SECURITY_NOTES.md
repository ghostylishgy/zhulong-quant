# Public Release Security Notes

The public repository is a curated source snapshot. Production state and
development history are kept separately.

Never commit environment files, API tokens, cookies, SSH material, backup
passwords, cloud authentication state, databases, vector stores, downloaded
market data, reports, logs, backup repositories, real holdings, cost bases,
account identifiers, private watchlists, machine names, private addresses, or
recovery evidence.

Addresses in the public source use RFC 5737 documentation ranges and are not
deployment targets. Configure all real service locations outside Git.

Deleting a leaked value from a later commit is not sufficient. Rotate exposed
credentials and rewrite public history when necessary.
