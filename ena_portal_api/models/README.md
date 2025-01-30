# Guide: Adding a New Model to ENA API Handler

This guide explains how to add a new model to the ENA API Handler. We'll use examples from existing models like Study and ReadRun to demonstrate the process.

## Prerequisites

Before starting, ensure you understand:
- The ENA Portal API structure
- Python Pydantic models
- Python Enums

## Steps to Add a New Model

### 1. Identify the Model Type

First, identify your new model type from the `ENAPortalResultType` enum. For example:
```python
class ENAPortalResultType(str, Enum):
    ANALYSIS = "analysis"
    ASSEMBLY = "assembly"
    SAMPLE = "sample"
    # ... etc
```

### 2. Create Fields Enum

Create a new enum class in your model's file (e.g., `models/assembly.py`) that inherits from `str, Enum`. This will define all available fields for your model:

```python
from enum import Enum

class ENAAssemblyFields(str, Enum):
    ASSEMBLY_ACCESSION = "assembly_accession"
    ASSEMBLY_NAME = "assembly_name"
    ASSEMBLY_LEVEL = "assembly_level"
    # Add all available fields
```

Important notes:
- Inherit from `str, Enum` to ensure Pydantic compatibility
- Use UPPERCASE for enum names (Python convention)
- Use lowercase with underscores for values (ENA API convention)
- Order fields alphabetically
- Add descriptive comments for each field

### 3. Create Query Model

Create a query class that inherits from `BaseENAQueryConditions`:

```python
from datetime import date
from typing import Optional
from pydantic import Field
from ..query.base import BaseENAQueryConditions

class ENAAssemblyQuery(BaseENAQueryConditions):
    assembly_accession: Optional[str] = Field(None, description="Assembly accession number")
    assembly_name: Optional[str] = Field(None, description="Assembly name")
    assembly_level: Optional[str] = Field(None, description="Assembly level")
    # Add all queryable fields
```

Important notes:
- Make fields Optional (Except for required fields)
- Use appropriate types (str, int, date, etc.)
- Add descriptions using Field()
- Use the same field names as in the Fields enum but in lowercase
- Order fields alphabetically to match the Fields enum

### 4. Update ENAAPIRequest

Update the ENAAPIRequest class to handle your new model:

1. Add your new Fields type to the fields Union:
```python
fields: Union[
    List[
        Union[
            ENAAnalysisFields,
            ENAAssemblyFields,
            ENASampleFields,
            ENAReadRunFields,
            # Add your new Fields type
        ]
    ], 
   # Add your new fields
]
```

### 5. Testing Your New Model

Create a test to verify your new model works:

```python
try:
    request = ENAAPIRequest(
        result=ENAPortalResultType.ASSEMBLY,
        fields=[
            ENAAssemblyFields.ASSEMBLY_ACCESSION,
            ENAAssemblyFields.ASSEMBLY_NAME,
            ENAAssemblyFields.ASSEMBLY_LEVEL,
        ],
        limit=10
    )
    response = request.get()
    print(response.text)
except ValueError as e:
    print(f"Error fetching assemblies: {e}")
```

## Common Issues and Solutions

1. **Pydantic Validation Errors**
   - Ensure your Fields enum inherits from `str, Enum`
   - Make sure all optional fields have `Optional[type]` annotation
   - Use proper Field() descriptions

2. **API Errors**
   - Verify field names match exactly with ENA API
   - Check query syntax is correct
   - Ensure field values are properly formatted

3. **Type Errors**
   - Update all Union types in ENAAPIRequest
   - Add proper validation in model_validator
   - Use correct types for each field

## Best Practices

1. **Naming Conventions**
   - PascalCase for classes (ENAAssemblyQuery)
   - UPPERCASE for enum members (ASSEMBLY_LEVEL)
   - lowercase_with_underscores for field names (assembly_level)

2. **Documentation**
   - Add descriptions to all fields
   - Document any special validation rules
   - Include example usage

## Example Implementation

Here's a complete example of a new model implementation:

```python
# models/assembly.py
from datetime import date
from enum import Enum
from typing import Optional
from pydantic import Field, model_validator

from ..query.base import BaseENAQueryConditions

class AssemblyFields(str, Enum):
    ASSEMBLY_ACCESSION = "assembly_accession"
    ASSEMBLY_NAME = "assembly_name"
    ASSEMBLY_LEVEL = "assembly_level"
    COVERAGE = "coverage"

class AssemblyQuery(BaseENAQueryConditions):
    assembly_accession: Optional[str] = Field(None, description="Assembly accession number")
    assembly_name: Optional[str] = Field(None, description="Assembly name")
    assembly_level: Optional[str] = Field(None, description="Assembly level (chromosome, scaffold, contig)")
    coverage: Optional[float] = Field(None, description="Sequencing coverage")

    @model_validator(mode="after")
    def validate_assembly_level(self):
        if self.assembly_level and self.assembly_level not in ["chromosome", "scaffold", "contig"]:
            raise ValueError("assembly_level must be one of: chromosome, scaffold, contig")
        return self
```