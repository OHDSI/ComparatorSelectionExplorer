# @file PackageMaintenance
#
# Copyright 2025 Observational Health Data Sciences and Informatics
#
# This file is part of CohortGenerator
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Format and check code
OhdsiRTools::checkUsagePackage("ComparatorSelectionExplorer")
OhdsiRTools::updateCopyrightYearFolder()
devtools::document()

# Create manual and vignettes:
unlink("extras/ComparatorSelectionExplorer.pdf")
system2("R", args = c("CMD", "Rd2pdf", "./", "--output=extras/ComparatorSelectionExplorer.pdf"))

dir.create(path = file.path(getwd(), "inst", "doc"), showWarnings = FALSE)
purrr::walk(list.files("vignettes", pattern = "*.Rmd", full.names = TRUE), function(filepath) {
  output <- file.path(getwd(), "inst", "doc", paste0(gsub(".Rmd", "", basename(filepath)), ".pdf"))
  rmarkdown::render(filepath,
                    output_file = output,
                    rmarkdown::pdf_document(latex_engine = "pdflatex",
                                            toc = TRUE,
                                            number_sections = TRUE))

})
