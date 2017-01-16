package org.apache.spark.sql.execution.datasources.hbase.annotation;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.List;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.Element;

import javax.tools.Diagnostic.Kind;

import com.sun.source.tree.AnnotationTree;
import com.sun.source.tree.ExpressionTree;
import com.sun.source.tree.MethodTree;
import com.sun.source.tree.StatementTree;
import com.sun.source.tree.Tree;
import com.sun.source.tree.VariableTree;
import com.sun.source.util.SimpleTreeVisitor;
import com.sun.source.util.Trees;


public class ShcAnnotationProcessor extends AbstractProcessor {
	private Trees m_tree;	// Trees object for a given CompilationTask
	private CatalogAnnotationVisitor m_visitor;
	
	public ShcAnnotationProcessor() {
	}

	@Override
	public synchronized void init(ProcessingEnvironment processingEnv) {
		super.init(processingEnv);
		m_tree = Trees.instance(processingEnv);
		m_visitor = new CatalogAnnotationVisitor(processingEnv);
	}

	@Override
	public Set<String> getSupportedAnnotationTypes() {
		Set<String> annotataions = new LinkedHashSet<String>();
		annotataions.add(ShcHbaseCatalogDef.class.getCanonicalName());
		return annotataions;
	}

	@Override
	public SourceVersion getSupportedSourceVersion() {
		return SourceVersion.latestSupported();
	}
	
	// In Java SE8, annotations on local variables cannot be retrieved directly.
	// Method level is the best we can get. From there, we'll take advantage of AST
	public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
		Set<? extends Element> elements = roundEnv.getElementsAnnotatedWith(ShcHbaseCatalogDef.class);
		
		for(Element element : elements) {
			// Must be METHOD
			if(element.getKind() == ElementKind.METHOD) {
				MethodTree mt = (MethodTree)m_tree.getTree(element);
				List<? extends StatementTree> statmentTrees = mt.getBody().getStatements();
				for(StatementTree st : statmentTrees) {
					st.accept(m_visitor, element); // access each statement
				}					
			}
		}

		return true;
	}
}


class CatalogAnnotationVisitor extends SimpleTreeVisitor<Void, Element> {
	private ProcessingEnvironment m_processingEnv;
	
	public CatalogAnnotationVisitor(ProcessingEnvironment processingEnv) {
		m_processingEnv = processingEnv;
	}

	@Override
	public Void visitVariable(VariableTree vt, Element element) {
		if(containsCatalogAnnotation(vt.getModifiers().getAnnotations())) {
			ExpressionTree et = vt.getInitializer();
			if(et.getKind() != Tree.Kind.STRING_LITERAL) {
				m_processingEnv.getMessager().printMessage(Kind.ERROR,
						"local varible \"" + vt.getType() + " " + vt.getName() + "\": " +
						"@" + ShcHbaseCatalogDef.class.getSimpleName() + " " +
						"can only be used by a String variable initialized with a " +
						"string literal.", element);
			}
			// TODO: Parse string
			System.out.println(vt.getInitializer());
		}
		return super.visitVariable(vt, element);
	}
	
	
	private boolean containsCatalogAnnotation(List<? extends AnnotationTree> annotationTreeList)
	{
		for(Iterator<? extends AnnotationTree> it = annotationTreeList.iterator(); it.hasNext(); ) {
			AnnotationTree at = it.next();
			if(at.getAnnotationType().toString().contains(ShcHbaseCatalogDef.class.getSimpleName()))
				return true;
		}
		return false;
	}
}
