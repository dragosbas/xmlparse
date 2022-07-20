
# Python Revisal & D112  Parse

This Python based Rest WebApi that handles incoming Standard Revisal Standard Files AND D112 files and returns their content as Json, SQL or XLSX file.
Note: All individual private informations are anonimized




## Authors

- [@dragosbas](https://github.com/dragosbas)


## Tech Stack

**Client:** Html

**Server:** Python, Flask, Pandas


## API Reference

#### Parse File


```http
  POST /
```

| Parameter   | Type     | Description                       |
| :--------   | :------- | :-------------------------------- |
| `filename`  | `file`   | **Required**. File to be parsed   |
| `companyCui`| `string` | File CUI Ownership Validation     |
| `reportDate'| `DATE`   | MM/YY Validation                  |
| `corExclus' | `string` | COR values to be excluded         |
| `cnp1'      | `string` | CNP owners to be excluded         |
| `cnp2'      | `string` | SHA256 COR values to be excluded  |
|`minCor`|`string`|Minimum number of employees per cor to be imported|




## Deployment

To deploy this project

1. Clone the repo
```bash
git clone https://github.com/R0bert196/CodecoolShop
```
2. Install all requirements from requirements.txt

```bash
  pip install -r requirements.txt
```

3. Compile and run !


## License

[MIT](https://choosealicense.com/licenses/mit/)

