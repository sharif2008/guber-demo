import { Job } from "bullmq"
import { countryCodes, dbServers, EngineType } from "../config/enums"
import { ContextType } from "../libs/logger"
import { jsonOrStringForDb, jsonOrStringToJson, stringOrNullForDb, stringToHash } from "../utils"
import _ from "lodash"
import { sources } from "../sites/sources"
import items from "../../pharmacyItems.json"
import connections from "../../brandConnections.json"

//types
type BrandsMapping = {
    [key: string]: string[]
}
type BrandGroupInfo = {
    canonicalBrand: string
    allBrands: Set<string>
}
//common functions

function normalizeBrandName(brand: string): string {
    return brand
        .normalize("NFD")
        .replace(/[\u0300-\u036f]/g, "")
        .toLowerCase()
        .trim()
}

function shouldIgnoreBrand(brand: string): boolean {
    const normalizedBrand = brand.toLowerCase().trim()
    const ignoreList = ["bio", "neb"]
    return ignoreList.includes(normalizedBrand)
}

const FRONT_ONLY_BRANDS = new Set([
    "rich", "rff", "flex", "ultra", "gum", "beauty", "orto", "free", "112", "kin", "happy"
])

const FRONT_OR_SECOND_BRANDS = new Set([
    "heel", "contour", "nero", "rsv"
])

function checkBrandPosition(title: string, brand: string, matchIndex: number): boolean {
    const lowerBrand = brand.toLowerCase()

    if (lowerBrand === "happy") {
        const words = title.split(/\s+/)
        return words[matchIndex] === "HAPPY" || words[matchIndex] === "Happy"
    }

    if (FRONT_ONLY_BRANDS.has(lowerBrand)) {
        return matchIndex === 0
    }

    if (FRONT_OR_SECOND_BRANDS.has(lowerBrand)) {
        return matchIndex === 0 || matchIndex === 1
    }

    return true
}
//task 2 details : build a commong name for all brands that are related to each other in a group
function buildBrandGroups(brandsMapping: BrandsMapping): Map<string, BrandGroupInfo> {
    const brandGroups = new Map<string, BrandGroupInfo>()
    const visited = new Set<string>()

    for (const brand in brandsMapping) {
        if (visited.has(brand)) {
            continue
        }

        const group = new Set<string>()
        const toProcess = [brand]

        while (toProcess.length > 0) {
            const current = toProcess.pop()!
            if (visited.has(current) || !brandsMapping[current]) {
                continue
            }

            visited.add(current)
            group.add(current)

            for (const related of brandsMapping[current]) {
                if (!visited.has(related)) {
                    toProcess.push(related)
                }
            }
        }

        const canonicalBrand = Array.from(group).sort((a, b) => {
            return a.length !== b.length ? a.length - b.length : a.localeCompare(b)
        })[0]

        for (const b of group) {
            brandGroups.set(b, {
                canonicalBrand,
                allBrands: group
            })
        }
    }
    //console.log(brandGroups);
    console.log("brandGroups length: ", brandGroups.size);
    return brandGroups
}

export async function getBrandsMapping(): Promise<BrandsMapping> {
    const brandConnections = connections

    // Create a map to track brand relationships
    const brandMap = new Map<string, Set<string>>()

    brandConnections.forEach(({ manufacturer_p1, manufacturers_p2 }) => {
        const brand1 = manufacturer_p1.toLowerCase()
        const brands2 = manufacturers_p2.toLowerCase()
        const brand2Array = brands2.split(";").map((b) => b.trim())
        if (!brandMap.has(brand1)) {
            brandMap.set(brand1, new Set())
        }
        brand2Array.forEach((brand2) => {
            if (!brandMap.has(brand2)) {
                brandMap.set(brand2, new Set())
            }
            brandMap.get(brand1)!.add(brand2)
            brandMap.get(brand2)!.add(brand1)
        })
    })

    // Convert the flat map to an object for easier usage
    const flatMapObject: Record<string, string[]> = {}

    brandMap.forEach((relatedBrands, brand) => {
        flatMapObject[brand] = Array.from(relatedBrands)
    })

    return flatMapObject
}

async function getPharmacyItems(countryCode: countryCodes, source: sources, versionKey: string, mustExist = true) {
    const finalProducts = items

    return finalProducts
}


//task 1 details
function findBrandMatches(title: string, brand: string): Array<{ brand: string; matchIndex: number }> {
    const matches: Array<{ brand: string; matchIndex: number }> = []
    const words = title.split(/\s+/)
    const normalizedBrand = normalizeBrandName(brand)
    const originalBrandLower = brand.toLowerCase()

    words.forEach((word, index) => {
        const normalizedWord = normalizeBrandName(word)

        if (word === brand || word.toLowerCase() === originalBrandLower) {
            matches.push({ brand, matchIndex: index })
            return
        }

        if (normalizedWord === normalizedBrand) {
            matches.push({ brand, matchIndex: index })
            return
        }

        const escapedBrand = originalBrandLower.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")
        const wordBoundaryRegex = new RegExp(`\\b${escapedBrand}\\b`, "i")
        if (wordBoundaryRegex.test(word) && normalizedWord.includes(normalizedBrand)) {
            matches.push({ brand, matchIndex: index })
        }
    })

    return matches
}

// if >1 brands matched, prioritize matching beginning
function checkBrandMatch(title: string, brand: string): { isValid: boolean; matchIndex?: number } {
    if (shouldIgnoreBrand(brand)) {
        return { isValid: false }
    }

    const matches = findBrandMatches(title, brand)
    if (matches.length === 0) {
        return { isValid: false }
    }

    const validMatches = matches.filter(m => checkBrandPosition(title, m.brand, m.matchIndex))
    if (validMatches.length === 0) {
        return { isValid: false }
    }

    const bestMatch = validMatches.find(m => m.matchIndex === 0) || validMatches[0]
    return { isValid: true, matchIndex: bestMatch.matchIndex }
}
//deprecated function
export function checkBrandIsSeparateTerm(input: string, brand: string): boolean {
    // Escape any special characters in the brand name for use in a regular expression
    const escapedBrand = brand.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")

    // Check if the brand is at the beginning or end of the string
    const atBeginningOrEnd = new RegExp(
        `^(?:${escapedBrand}\\s|.*\\s${escapedBrand}\\s.*|.*\\s${escapedBrand})$`,
        "i"
    ).test(input)

    // Check if the brand is a separate term in the string
    const separateTerm = new RegExp(`\\b${escapedBrand}\\b`, "i").test(input)

    // The brand should be at the beginning, end, or a separate term
    return atBeginningOrEnd || separateTerm
}

export async function assignBrandIfKnown(countryCode: countryCodes, source: sources, job?: Job) {
    const context = { scope: "assignBrandIfKnown" } as ContextType
    const brandsMapping = await getBrandsMapping()
    const brandGroups = buildBrandGroups(brandsMapping); //for task 2

    const start = Date.now();

    const versionKey = "assignBrandIfKnown"
    let products = await getPharmacyItems(countryCode, source, versionKey, false)
    let counter = 0
    let matchedCount = 0

    for (let product of products) {
        counter++

        if (product.m_id) {
            // Already exists in the mapping table, probably no need to update
            continue
        }

        const brandMatches: Array<{ brand: string; matchIndex: number; isBeginning: boolean }> = []

        //removed nested loop, because of built the brand groups in task 2
        for (const brand of brandGroups.keys()) {
            //task 1 details: check if the brand is in the title
            const matchResult = checkBrandMatch(product.title, brand)

            if (matchResult.isValid) {
                matchedCount++;
                brandMatches.push({
                    brand,
                    matchIndex: matchResult.matchIndex!,
                    isBeginning: matchResult.matchIndex === 0
                })
            }
        }

        if (brandMatches.length === 0) {
            console.log(`${product.title} -> no match`)
            continue
        }

        brandMatches.sort((a, b) => {
            if (a.isBeginning !== b.isBeginning) {
                return a.isBeginning ? -1 : 1
            }
            return a.matchIndex - b.matchIndex
        })

        const bestMatch = brandMatches[0]
        const groupInfo = brandGroups.get(bestMatch.brand)
        const finalBrand = groupInfo ? groupInfo.canonicalBrand : bestMatch.brand

        console.log(`${product.title} -> ${finalBrand}`)

        const sourceId = product.source_id
        const key = `${source}_${countryCode}_${sourceId}`
        const uuid = stringToHash(key)

        // Then brand is inserted into product mapping table
    }
    const elapsed = Date.now() - start;
    console.log(`Execution time: ${elapsed} ms`);
    console.log(`Processed ${counter} products, matched ${matchedCount} brands`)
}