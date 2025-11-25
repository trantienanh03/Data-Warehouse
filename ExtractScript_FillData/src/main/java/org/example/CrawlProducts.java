package org.example;

import org.openqa.selenium.*;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

import javax.swing.JFileChooser;
import javax.swing.UIManager;
import javax.swing.filechooser.FileNameExtensionFilter;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

public class CrawlProducts {

    public static String getVariableFromCSV(String csvPath, String variableName) throws Exception {
        try (BufferedReader br = new BufferedReader(new FileReader(csvPath))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",", 2);
                if (parts.length == 2 && parts[0].trim().equals(variableName)) {
                    return parts[1].replace("\"", "").trim();
                }
            }
        }
        throw new RuntimeException("Variable not found: " + variableName);
    }

    public static void main(String[] args) throws Exception {

        System.setOut(new PrintStream(System.out, true, StandardCharsets.UTF_8));
        System.setErr(new PrintStream(System.err, true, StandardCharsets.UTF_8));

        UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
        JFileChooser fileChooser = new JFileChooser();
        fileChooser.setDialogTitle("Select variable.csv file");
        fileChooser.setFileFilter(new FileNameExtensionFilter("CSV Files", "csv"));
        int result = fileChooser.showOpenDialog(null);
        String variableCsvPath;
        if (result == JFileChooser.APPROVE_OPTION) {
            variableCsvPath = fileChooser.getSelectedFile().getAbsolutePath();
        } else {
            throw new RuntimeException("No CSV file selected.");
        }

        String driverPath = getVariableFromCSV(variableCsvPath, "driverPath");
        System.setProperty("webdriver.chrome.driver", driverPath);

        ChromeOptions options = new ChromeOptions();
        String webBrowserPath = getVariableFromCSV(variableCsvPath, "webBrowserPath");
        options.setBinary(webBrowserPath);
        options.addArguments("--disable-blink-features=AutomationControlled");
        options.addArguments("--start-maximized");

        WebDriver driver = new ChromeDriver(options);
        WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(15));

        String csvOutputPath = getVariableFromCSV(variableCsvPath, "csvPath");
        Files.createDirectories(Paths.get(csvOutputPath).getParent());

        String url = getVariableFromCSV(variableCsvPath, "url");
        System.out.println("URL: " + url); // Add this line to debug

        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(csvOutputPath), StandardCharsets.UTF_8)) {

            writer.write(
                    "name,screenSize,screenTechnology,screenResolution,camera,chipset,ram,storage,battery,version,color,price,oldPrice,link,rating,numReview,nfc\n");

            driver.get(url);
            Thread.sleep(1500);

            List<String> links = new ArrayList<>();
            int productsNumber = 500;

            while (links.size() < productsNumber) {

                List<WebElement> products = driver
                        .findElements(By.cssSelector("div.product-info-container.product-item"));

                for (WebElement product : products) {
                    try {
                        String link = product.findElement(By.cssSelector("a.product__link")).getAttribute("href");
                        if (!links.contains(link))
                            links.add(link);
                    } catch (Exception ignored) {
                    }
                }

                System.out.println("Collected: " + links.size());

                if (links.size() >= productsNumber)
                    break;

                try {

                    // Close annoying email popup if shown
                    try {
                        WebElement popupClose = driver.findElement(By.cssSelector("button.cancel-button-top"));
                        if (popupClose.isDisplayed()) {
                            ((JavascriptExecutor) driver).executeScript("arguments[0].click();", popupClose);
                            Thread.sleep(600);
                            System.out.println("Popup closed!");
                        }
                    } catch (NoSuchElementException ignored) {
                    }

                    WebDriverWait waitPopup = new WebDriverWait(driver, Duration.ofSeconds(15));
                    WebElement loadMoreButton = waitPopup.until(
                            ExpectedConditions.visibilityOfElementLocated(
                                    By.xpath(
                                            "//a[contains(@class,'btn-show-more') and contains(@class,'button__show-more-product')]")));

                    ((JavascriptExecutor) driver).executeScript(
                            "window.scrollTo(0, arguments[0].getBoundingClientRect().top + window.scrollY - 200);",
                            loadMoreButton);
                    Thread.sleep(800);

                    try {
                        loadMoreButton.click();
                    } catch (ElementClickInterceptedException ex) {
                        ((JavascriptExecutor) driver).executeScript("arguments[0].click();", loadMoreButton);
                    }

                    Thread.sleep(1000);

                    waitPopup.until(ExpectedConditions.numberOfElementsToBeMoreThan(
                            By.cssSelector("div.product-info-container.product-item"),
                            products.size()));

                } catch (TimeoutException e) {
                    System.out.println("No more pages to load!");
                    break;
                } catch (Exception e) {
                    System.out.println("Unexpected Load More error: " + e.getMessage());
                    break;
                }
            }

            for (String link : links) {
                try {
                    driver.get(link);
                    wait.until(ExpectedConditions.visibilityOfElementLocated(By.cssSelector("h1")));
                    Thread.sleep(500);

                    String name = "";
                    try {
                        name = driver.findElement(By.cssSelector("div.box-product-name h1")).getText().trim();
                    } catch (Exception ignored) {
                    }

                    String version = "";
                    try {
                        List<WebElement> versionElems = driver.findElements(By.cssSelector("div.list-linked a"));
                        version = versionElems.stream()
                                .map(WebElement::getText)
                                .filter(s -> !s.isEmpty())
                                .collect(Collectors.joining(" "));
                    } catch (Exception ignored) {
                    }

                    String screenSize = extractSpec(driver, "Kích thước màn hình");
                    String screenTechnology = extractSpec(driver, "Công nghệ màn hình");
                    String price = extractPrice(driver, ".sale-price", ".field-final-price");
                    String oldPrice = extractPrice(driver, ".base-price", null);

                    String rating = getTextSafe(driver, ".box-rating span");
                    String numReviews = getTextSafe(driver, ".box-rating .total-rating").replaceAll("[^0-9]", "");

                    String screenResolution = extractSpec(driver, "Độ phân giải màn hình");
                    String camera = extractSpec(driver, "Camera sau");
                    String chipset = extractSpec(driver, "Chipset");
                    String ram = extractSpec(driver, "RAM");
                    String storage = extractSpec(driver, "Bộ nhớ trong");
                    String battery = extractSpec(driver, "Pin").replaceAll("[^0-9]", "");

                    String nfc = extractSpec(driver, "NFC");

                    String color = driver.findElements(By.cssSelector(".box-product-variants .item-variant-name"))
                            .stream().map(WebElement::getText).collect(Collectors.joining(", "));

                    System.out.println("Done " + name);

                    writer.write("\"" + name + "\",\"" + screenSize + "\",\"" + screenTechnology + "\",\""
                            + screenResolution + "\",\"" +
                            camera + "\",\"" + chipset + "\",\"" + ram + "\",\"" + storage + "\",\"" + battery + "\",\""
                            + version + "\",\"" +
                            color + "\",\"" + price + "\",\"" + oldPrice + "\",\"" + link + "\",\"" +
                            rating + "\",\"" + numReviews + "\",\"" + nfc + "\"\n");

                } catch (Exception e) {
                    System.err.println(" Error processing product: " + link);
                }
            }
        } finally {
            driver.quit();
        }

        System.out.println("Crawl Done!");
    }

    private static String extractSpec(WebDriver driver, String label) {
        try {
            WebElement row = driver.findElement(By.xpath("//tr[td[contains(text(),'" + label + "')]]"));
            return row.findElement(By.xpath("./td[2]//*[self::p or self::a or self::span]")).getText().trim();
        } catch (Exception ignored) {
        }
        return "";
    }

    private static String extractPrice(WebDriver driver, String selector, String fallback) {
        try {
            String txt = driver.findElement(By.cssSelector(selector)).getText();
            return txt.replaceAll("[^0-9]", "");
        } catch (Exception ignored) {
            if (fallback != null)
                return extractPrice(driver, fallback, null);
        }
        return "";
    }

    private static String getTextSafe(WebDriver driver, String css) {
        try {
            return driver.findElement(By.cssSelector(css)).getText().trim();
        } catch (Exception ignored) {
        }
        return "";
    }
}
