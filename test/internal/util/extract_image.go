package util

import (
	"fmt"
	"strings"

	"github.com/russross/blackfriday/v2"
	"golang.org/x/net/html"
)

// ExtractImagesFromHTML 从html里提取图片地址
func ExtractImagesFromHTML(htmlCode string) []string {
	var images []string
	doc, err := html.Parse(strings.NewReader(htmlCode))
	if err != nil {
		fmt.Println("Failed to parse HTML:", err)
		return images
	}
	images = extractHTMLImage(doc)
	return images
}

func extractHTMLImage(n *html.Node) []string {
	var urls []string
	if n.Type == html.ElementNode && n.Data == "img" {
		for _, attr := range n.Attr {
			if attr.Key == "src" {
				urls = append(urls, attr.Val)
			}
		}
	}
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		urls = append(urls, extractHTMLImage(c)...)
	}
	return urls
}

// ExtractImagesFromMarkdown 从markdown里提取图片地址
func ExtractImagesFromMarkdown(markdownText string) []string {
	fmt.Println(markdownText)
	ast := blackfriday.New().Parse([]byte(markdownText))
	fmt.Println(ast)
	return extractImagesFromAST(ast)
}

func extractImagesFromAST(node *blackfriday.Node) []string {
	var images []string

	if node.Type == blackfriday.Image {
		images = append(images, string(node.LinkData.Destination))
	}

	for child := node.FirstChild; child != nil; child = child.Next {
		childImages := extractImagesFromAST(child)
		images = append(images, childImages...)
	}

	return images
}
